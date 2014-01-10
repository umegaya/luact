local ffi = require 'luact.ffiex'
local _M = {}
local C = ffi.C

ffi.cdef [[
#include <stdlib.h>
]]

_M.alloc = function (sz, gc)
	return C.malloc(sz)
end

_M.alloc_typed = function (ct, sz)
	--print(ct, sz)
	return ffi.cast(ct .. "*", _M.alloc((sz or 1) * ffi.sizeof(ct)))
end

_M.managed_alloc = function (sz)
	return ffi.gc(C.malloc(sz), C.free)
end

_M.managed_alloc_typed = function (ct, sz)
	return ffi.cast(ct .. "*", _M.allocManaged((sz or 1) * ffi.sizeof(ct)))
end

_M.realloc = function (p, sz)
	return C.realloc(p, sz)
end

_M.realloc_typed = function (ct, p, sz)
	return ffi.cast(ct .. "*", C.realloc(p, sz))
end

_M.free = function (p)
	C.free(p)
end

return _M
