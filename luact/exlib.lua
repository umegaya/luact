local data = debug.getinfo(1)
local root = {data.source:find('@(.+)/.+$')}
package.path = root[3]..'/lib/?.lua;'..package.path
-- print(package.path)

local _M = {}
local pulpo_package = require 'pulpo.package'
-- pulpo_package.DEBUG = true
-- add our defer module dependency to pulpo's packaga system
_M.LUACT_BUFFER = pulpo_package.create_runlevel({
	"luact.defer.writer_c", "luact.defer.pbuf_c",
})
-- this group depends on LUACT_BUFFER modules.
_M.LUACT_IO = pulpo_package.create_runlevel({
	"luact.defer.conn_c", "luact.defer.clock_c",
})

return _M
