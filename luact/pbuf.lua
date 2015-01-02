-- force preload luact.writer first, which is dependency of pbuf.
local writer = require 'luact.writer'
local ffi = require 'ffiex.init'

local require_on_boot = (require 'pulpo.package').require
local _M = require_on_boot 'luact.defer.pbuf_c'

-- predefine luact_rbuf_t, because it refers many other modules
ffi.cdef[[
	typedef struct luact_rbuf {
		char *buf;
		luact_bufsize_t max, used, hpos;
	} luact_rbuf_t;
]]

return _M
