-- actor main loop
local ffi = require 'ffiex'
local memory = require 'luact.memory'

ffi.cdef [[
typedef struct {
	
} worker_t;
]]

return function (args, shmp)
	shmp = memory.alloc_typed('worker_t')
end