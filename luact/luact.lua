local tp = require 'threadpool'
local serde = require 'serde'
local const = require 'const'
local _M = {}

_M.__call = function (self, source)
	local th = tp.get()
	local packed = serde.pack(source)
	th.queue:push(const.CMD_CREATE_ACTOR, pack)
end

return setmetatable({}, _M)
