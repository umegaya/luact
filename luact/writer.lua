local require_on_boot = (require 'pulpo.package').require
local _M = require_on_boot 'luact.defer.writer_c'

ffi.cdef[[
	typedef size_t luact_bufsize_t;
]]

-- write commands
local WRITER_DEACTIVATE = -1
local WRITER_NONE = 0
local WRITER_RAW = 1
local WRITER_VEC = 2

_M.WRITER_DEACTIVATE = WRITER_DEACTIVATE
_M.WRITER_NONE = WRITER_NONE
_M.WRITER_RAW = WRITER_RAW
_M.WRITER_VEC = WRITER_VEC

return _M
