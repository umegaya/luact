local data = debug.getinfo(1)
local root = {data.source:find('@(.+)/.+$')}
if not package.path:match(root[3]..'/lib/%?%.lua;') then
	package.path = root[3]..'/lib/?.lua;'..package.path
end
-- print(package.path)

local _M = {}
local pulpo_package = require 'pulpo.package'
-- add our defer module dependency to pulpo's packaga system
_M.LUACT_BUFFER = pulpo_package.create_runlevel({
	"luact.defer.writer_c", "luact.defer.pbuf_c", "luact.defer.vid_c"
})
-- this group depends on LUACT_BUFFER modules.
_M.LUACT_IO = pulpo_package.create_runlevel({
	"luact.defer.conn_c", "luact.defer.clock_c",
})
-- this group depends on clock module
_M.LUACT_DEPLOY = pulpo_package.create_runlevel({
	"luact.defer.deploy_c", "luact.defer.module_c"
})

return _M
