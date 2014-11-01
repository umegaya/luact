-- force preload luact.writer first, which is dependency of pbuf.
local writer = require 'luact.writer'

local require_on_boot = (require 'pulpo.package').require
local _M = require_on_boot 'luact.defer.pbuf_c'

return _M
