local ffi = require 'luact.ffiex'
local memory = require 'luact.memory'
local util = require 'luact.util'
local _M = {}
local C = ffi.C


-- cdefs
ffi.path "/usr/local/include/luajit-2.0"
if ffi.os == 'OSX' then
	-- they don't have built in stdarg.h, so we add path that includes it.
	ffi.search("/Applications/Xcode.app/Contents/Developer/usr", "stdarg.h", true) -- if found, add to ffi.path also
	-- disable all __asm alias (because luajit cannot find aliasing symbols)
	ffi.cdef "#define __asm(exp)"
end
ffi.cdef "#include <pthread.h>"	
ffi.cdef "#include <lauxlib.h>"
ffi.cdef "#include <lualib.h>"
ffi.cdef [[
typedef void *(*thread_proc_t)(void *);
typedef struct {
	pthread_t pt;
	lua_State *L;
	void *shm[1];
} thread_handle_t;
typedef struct {
	thread_handle_t **list;
	int size, used;
	pthread_mutex_t mtx[1];
} thread_manager_t;
typedef struct {
	thread_manager_t *manager;
	void *original;
	void **shm;
} thread_args_t;
]]

ffi.lcpp_cdef = nil

-- metatype
ffi.metatype("thread_manager_t", {
	__index = {
		insert = function (t, thread)
			C.pthread_mutex_lock(t.mtx)
			if t.size <= t.used then
				t:expand(t.size * 2)
			end
			t.list[t.used] = thread
			t.used = (t.used + 1)
			C.pthread_mutex_unlock(t.mtx)
			return t.used		
		end,
		remove = function (t, thread)
			C.pthread_mutex_lock(t.mtx)
			local found = false
			for i=0,t.used-1,1 do
				if (not found) and C.pthread_equal(t.list[i].pt, thread.pt) ~= 0 then
					found = true
				else
					t.list[i - 1] = t.list[i]
				end
			end
			t.list[t.used - 1] = nil
			t.used = (t.used - 1)
			C.pthread_mutex_unlock(t.mtx)			
		end,
		find = function (t, id)
			local found
			C.pthread_mutex_lock(t.mtx)
			for i=0,t.used-1,1 do
				if C.pthread_equal(t.list[i].pt, id) ~= 0 then
					found = t.list[i]
					break
				end
			end
			C.pthread_mutex_unlock(t.mtx)						
			return found
		end,
		fetch = function (t, f)
			C.pthread_mutex_lock(t.mtx)
			local r = f(t.list, t.used)
			C.pthread_mutex_unlock(t.mtx)
			return r
		end,
		__expand = function (t, newsize)
			-- only called from mutex locked block
			t.list = memory.realloc_typed("thread_handle_t", t.list, newsize)
			assert(t.list)
			t.size = newsize
		end,
	}
})


-- variables
local threads


-- methods
-- initialize thread module. created threads are initialized with manager
_M.init = function (manager)
	if manager then
		threads = manager
	else
		threads = memory.alloc_typed("thread_manager_t")
		threads.size, threads.used = tonumber(util.n_cpu()), 0
		threads.list = memory.alloc_typed("thread_handle_t*", threads.size)
	end
end

-- finalize. no need to call from worker
_M.fin = function ()
	if threads then
		if threads.list then
			for i=0,threads.used-1,1 do
				local t = threads.list[i]
				C.pthread_join(t.pt, rv)
				C.lua_close(t.L)
				memory.free(t)
			end
			memory.free(threads.list)
		end
		memory.free(threads)
	end
end

-- create thread. args must be cast-able as void *.
_M.create = function (proc, args)
	local L = C.luaL_newstate()
	assert(L ~= nil)
	C.luaL_openlibs(L)
	local r
	r = C.luaL_loadstring(L, ([[
	local ffi = require("luact.ffiex")
	local thread = require("luact.thread")
	local main = load(%q)
	local mainloop = function (p)
		local args = ffi.cast("thread_args_t*", p)
		thread.init(args.manager)
		return main(args.original, args.shm)
	end
	__mainloop__ = tonumber(ffi.cast("intptr_t", ffi.cast("void *(*)(void *)", mainloop)))
	]]):format(string.dump(proc)))
	if r ~= 0 then
		assert(false, "luaL_loadstring:" .. tostring(r) .. "|" .. ffi.string(C.lua_tolstring(L, -1, nil)))
	end
	r = C.lua_pcall(L, 0, 1, 0)
	if r ~= 0 then
		assert(false, "lua_pcall:" .. tostring(r) .. "|" .. ffi.string(C.lua_tolstring(L, -1, nil)))
	end

	C.lua_getfield(L, ffi.defs.LUA_GLOBALSINDEX, "__mainloop__")
	local mainloop = C.lua_tointeger(L, -1);
	C.lua_settop(L, -2);

	local th = memory.alloc_typed("thread_handle_t", 1)
	th.shm[0] = nil
	local t = ffi.new("pthread_t[1]")
	local argp = ffi.new("thread_args_t", {threads, args, th.shm})
	assert(C.pthread_create(t, nil, ffi.cast("thread_proc_t", mainloop), argp) == 0)
	th.pt = t[0]
	th.L = L
	threads:insert(th)
	return th
end

-- destroy thread.
_M.destroy = function (thread)
	local rv = ffi.new("void*[1]")
	C.pthread_join(thread.pt, rv)
	C.lua_close(thread.L)
	threads:remove(thread)
	memory.free(thread)
	return rv[0]
end
_M.join = _M.destroy

-- get current thread handle
_M.me = function ()
	local pt = C.pthread_self()
	return threads:find(pt)
end

-- check 2 thread handles (pthread_t) are same 
_M.equal = function (t1, t2)
	return C.pthread_equal(t1.pt, t2.pt) ~= 0
end

-- get shared memory of specified thread
-- thread routine is function f(args, ptr_to_shm:void *[1]), 
-- if thread routine set value to ptr_to_shm[0], then it can be access from all thread.
-- note that value which stores to ptr_to_shm need to be alloced, no gc
_M.shm = function (thread, ct)
	local p = thread.shm[0]
	return (ct and p) and ffi.cast(ct .. "*", p) or p
end

-- iterate all current thread
_M.fetch = function (fn)
	return threads:fetch(fn)
end

-- nanosleep
_M.sleep = function (sec)
	-- convert to nsec
	local round = math.floor(sec)
	local req,rem = ffi.new('struct timespec[1]'), ffi.new('struct timespec[1]')
	req[0].tv_sec = round
	req[0].tv_nsec = math.floor((sec - round) * (1000 * 1000 * 1000))
	while C.nanosleep(req, rem) ~= 0 do
		local tmp = req
		req = rem
		rem = tmp
	end
end

return _M
