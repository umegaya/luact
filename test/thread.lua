-- package.path = ("../ffiex/?.lua;" .. package.path)
local ffi = require 'ffiex'
local util = require 'luact.util'
local memory = require 'luact.memory'
local thread = require 'luact.thread'

thread.init()

print('----- test1 -----')
local args = memory.alloc_typed('int', 3)
args[0] = 1
args[1] = 2
args[2] = 3
local t = thread.create(function (targs, shmp)
	-- because actually different lua_State execute the code inside this function, 
	-- ffi setup required again.
	local ffi = require 'ffi'
	ffi.cdef[[
	void *malloc(size_t sz);
	void free(void *p);
	]]
	local a = ffi.cast('int*', targs)
	print('hello from new thread:' .. (a[0] + a[1] + a[2]))
	assert((a[0] + a[1] + a[2]) == 6, "correctly passed values into new thread")
	ffi.C.free(targs)
	local r = ffi.cast('int*', ffi.C.malloc(ffi.sizeof('int[1]')))
	r[0] = 111
	return r
end, args)

local r = ffi.cast('int*', thread.destroy(t))
assert(r[0] == 111)




print('----- test2 -----')
local threads = {}
for i=0,util.n_cpu() - 1,1 do
	local a = memory.alloc_typed('int', 1)
	a[0] = (i + 1) * 111
	local t = thread.create(function (targs, shmp)
		local ffi = require 'ffi'
		ffi.cdef "unsigned int sleep(unsigned int seconds);"
		local thread = require 'luact.thread'
		local memory = require 'luact.memory'
		local p = memory.alloc_typed('int', 1)
		p[0] = (ffi.cast('int*', targs))[0]
		print('thread:', thread.me(), p[0])
		shmp[0] = p -- set this pointer as shared memory
		while p[0] > 0 do
			thread.sleep(0.1)
		end
		memory.free(p)
	end, a)
	table.insert(threads, t)
end
local finished = {}
while true do 
	local success = true
	thread.fetch(function (thread_list, size)
		assert(size == util.n_cpu(), "should be same size as created num")
		for i=0,size-1,1 do
			if not finished[i] then
				assert(thread.equal(threads[i+1], thread_list[i]), "thread handle will be same")
				local pi = thread.shm(thread_list[i], 'int')
				if not pi then
					success = false
					break
				else
					finished[i] = true
				end
				assert(pi[0] == ((i + 1) * 111), "can read shared memory correctly")
				pi[0] = 0 --> indicate worker thread to stop
			end
		end
	end)
	if success then
		break
	end
	thread.sleep(0.1)
end

thread.fin()

