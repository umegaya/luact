--package.path=("../ffiex/?.lua;" .. package.path)
local ffi = require 'ffiex'
local memory = require 'luact.memory'
local loader = require 'luact.loader'
local util = require 'luact.util'
local fs = require 'luact.fs'

local _M = {}
local C = ffi.C
local PT = ffi.load('pthread')

-- cdefs
ffi.cdef [[
	typedef struct thread_args_dummy {
		void *manager;
		void *cache;
		void *original;
		void **shm;
	} thread_args_dummy_t;
]]

function _M.init_cdef(cache)
	loader.initialize(cache)

	loader.load("thread.lua.pthread", {
		--> from pthread
		"pthread_t", "pthread_mutex_t", 
		"pthread_mutex_lock", "pthread_mutex_unlock", 
		"pthread_mutex_init", "pthread_mutex_destroy",
		"pthread_create", "pthread_join", "pthread_self",
		"pthread_equal", 
		--> from time
		"nanosleep",
	}, {}, "pthread", [[
		#include <pthread.h>
		#include <time.h>
	]])

	local ffi_state = loader.load("thread.lua.luaAPI", {
		--> from luauxlib, lualib
		"luaL_newstate", "luaL_openlibs",
		"luaL_loadstring", "lua_pcall", "lua_tolstring",
		"lua_getfield", "lua_tointeger",
		"lua_settop", "lua_close", 
	}, {
		"LUA_GLOBALSINDEX",
	}, nil, [[
		#include <lauxlib.h>
		#include <lualib.h>
	]])

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
			pthread_mutex_t load_mtx[1];
		} thread_manager_t;
		typedef struct {
			thread_manager_t *manager;
			c_parsed_info_t *parsed;
			void *original;
			void **shm;
		} thread_args_t;
	]]

	_M.defs = {
		LUA_GLOBALSINDEX = ffi_state.defs.LUA_GLOBALSINDEX
	}

	--> metatype
	ffi.metatype("thread_manager_t", {
		__index = {
			init = function (t)
				t.size, t.used = tonumber(util.n_cpu()), 0
				t.list = assert(memory.alloc_typed("thread_handle_t*", t.size), 
					"fail to allocation thread list")
				PT.pthread_mutex_init(t.mtx, nil)
				PT.pthread_mutex_init(t.load_mtx, nil)
			end,
			fin = function (t)
				if t.list then
					for i=0,t.used-1,1 do
						local th = t.list[i]
						PT.pthread_join(th.pt, rv)
						C.lua_close(th.L)
						memory.free(th)
					end
					memory.free(t.list)
				end
				PT.pthread_mutex_destroy(t.mtx)
				PT.pthread_mutex_destroy(t.load_mtx)
			end,
			insert = function (t, thread)
				PT.pthread_mutex_lock(t.mtx)
				if t.size <= t.used then
					t:expand(t.size * 2)
					if t.size <= t.used then
						return nil
					end
				end
				t.list[t.used] = thread
				t.used = (t.used + 1)
				PT.pthread_mutex_unlock(t.mtx)
				return t.used		
			end,
			remove = function (t, thread)
				PT.pthread_mutex_lock(t.mtx)
				local found = false
				for i=0,t.used-1,1 do
					if (not found) and PT.pthread_equal(t.list[i].pt, thread.pt) ~= 0 then
						found = true
					else
						t.list[i - 1] = t.list[i]
					end
				end
				t.list[t.used - 1] = nil
				t.used = (t.used - 1)
				PT.pthread_mutex_unlock(t.mtx)			
			end,
			find = function (t, id)
				local found
				PT.pthread_mutex_lock(t.mtx)
				for i=0,t.used-1,1 do
					if PT.pthread_equal(t.list[i].pt, id) ~= 0 then
						found = t.list[i]
						break
					end
				end
				PT.pthread_mutex_unlock(t.mtx)						
				return found
			end,
			fetch = function (t, f)
				PT.pthread_mutex_lock(t.mtx)
				local r = f(t.list, t.used)
				PT.pthread_mutex_unlock(t.mtx)
				return r
			end,
			expand = function (t, newsize)
				-- only called from mutex locked bloc
				local p = memory.realloc_typed("thread_handle_t", newsize)
				if p then
					t.list = p
					t.size = newsize
				else
					print('expand thread list fails:'..newsize)
				end
				return p
			end,
		}
	})

	--> preserve parser result for faster startup of second thread
	_M.c_parsed_info = c_parsed_info
	_M.req,_M.rem = ffi.new('struct timespec[1]'), ffi.new('struct timespec[1]')
end


-- variables
local threads


-- methods
-- initialize thread module. created threads are initialized with manager
function _M.initialize(opts)
	_M.apply_options(opts or {})
	_M.init_cdef()
	threads = memory.alloc_typed("thread_manager_t")
	threads:init()
end
function _M.apply_options(opts)
	_M.opts = opts
	if opts.cdef_cache_dir then
		util.mkdir(opts.cdef_cache_dir)
		loader.set_cache_dir(opts.cdef_cache_dir)
	end
end

function _M.init_worker(manager, cache)
	assert(manager and cache)
	_M.init_cdef(ffi.cast("c_parsed_info_t*", cache))
	threads = ffi.cast("thread_manager_t*", manager)
end

function _M.load(name, cdecls, macros, lib, from)
	PT.pthread_mutex_lock(threads.load_mtx)
	loader.load(name, cdecls, macros, lib, from)
	PT.pthread_mutex_unlock(threads.load_mtx)	
end

-- finalize. no need to call from worker
function _M.fin()
	if threads then
		threads:fin()
		memory.free(threads)
		loader.finalize()
	end
end

-- create thread. args must be cast-able as void *.
function _M.create(proc, args)
	local L = C.luaL_newstate()
	assert(L ~= nil)
	C.luaL_openlibs(L)
	local r
	r = C.luaL_loadstring(L, ([[
	local thread = require("luact.thread")
	local main = load(%q)
	local mainloop = function (p)
		local args = ffi.cast("thread_args_dummy_t*", p)
		thread.init_worker(args.manager, args.cache)
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

	C.lua_getfield(L, _M.defs.LUA_GLOBALSINDEX, "__mainloop__")
	local mainloop = C.lua_tointeger(L, -1)
	C.lua_settop(L, -2)

	local th = memory.alloc_typed("thread_handle_t", 1)
	th.shm[0] = nil
	local t = ffi.new("pthread_t[1]")
	local argp = ffi.new("thread_args_t", {threads, loader.get_cache_ptr(), args, th.shm})
	assert(PT.pthread_create(t, nil, ffi.cast("thread_proc_t", mainloop), argp) == 0)
	th.pt = t[0]
	th.L = L
	return threads:insert(th) and th or nil
end

-- destroy thread.
function _M.destroy(thread)
	local rv = ffi.new("void*[1]")
	PT.pthread_join(thread.pt, rv)
	C.lua_close(thread.L)
	threads:remove(thread)
	memory.free(thread)
	return rv[0]
end
_M.join = _M.destroy

-- get current thread handle
function _M.me()
	local pt = PT.pthread_self()
	return threads:find(pt)
end

-- check 2 thread handles (pthread_t) are same 
function _M.equal(t1, t2)
	return PT.pthread_equal(t1.pt, t2.pt) ~= 0
end

-- get shared memory of specified thread
-- thread routine is function f(args, ptr_to_shm:void *[1]), 
-- if thread routine set value to ptr_to_shm[0], then it can be access from all thread.
-- note that value which stores to ptr_to_shm need to be alloced, no gc
function _M.shm(thread, ct)
	local p = thread.shm[0]
	return (ct and p ~= ffi.NULL) and ffi.cast(ct .. "*", p) or nil
end

-- iterate all current thread
function _M.fetch(fn)
	return threads:fetch(fn)
end

-- nanosleep
function _M.sleep(sec)
	-- convert to nsec
	local req, rem = _M.req, _M.rem
	util.sec2timespec(sec, _M.req)
	while C.nanosleep(req, rem) ~= 0 do
		local tmp = req
		req = rem
		rem = tmp
	end
end

return _M
