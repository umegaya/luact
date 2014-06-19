local loader = require 'luact.loader'
local ffi = require 'ffiex'
local _M = {}
local ffi_state

loader.add_lazy_initializer(function ()
	ffi_state = loader.load("errno.lua", {}, {
		"EAGAIN", "EWOULDBLOCK", "ENOTCONN", "EINPROGRESS", "EPIPE", 
		regex = {
			"^E%w+"
		}
	}, nil, [[
		#include <errno.h>
	]])
end)

function _M.errno()
	return ffi.errno()
end

return setmetatable(_M, {
	__index = function (t, k)
		local v = ffi_state.defs[k]
		assert(v, "no error definition:"..k)
		rawset(t, k, v)
		return v
	end
})
