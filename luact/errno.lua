local loader = require 'luact.loader'
local ffi = require 'ffiex'

local ffi_state = loader.load("errno.lua", {}, {
	"EAGAIN", "EWOULDBLOCK", "ENOTCONN", "EINPROGRESS", 
}, nil, [[
	#include <errno.h>
]])

return {
	EAGAIN = ffi_state.defs.EAGAIN, 
	EWOULDBLOCK = ffi_state.defs.EWOULDBLOCK, 
	ENOTCONN = ffi_state.defs.ENOTCONN, 
	EINPROGRESS = ffi_state.defs.EINPROGRESS, 
	errno = function ()
		return tonumber(ffi.errno())
	end,
}
