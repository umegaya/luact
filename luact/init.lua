local util = require 'luact.util'
local thread = require 'luact.thread'
local serde = require 'luact.serde'
local actor = require 'luact.actor'
local _M = {}
local shms = {}

thread.init()

-- internal methods
local new_worker = function ()
end

local get_worker = function ()
end


-- exported methods
_M.config = {
	worker = util.n_cpu()
}
_M.__call = function (self, source)
	if is_actor(source) then return source end
	actor.uuid(source)
	local th, q = get_worker()
	local ok, r
	q:lock()
	ok, r = pcall(serde.ser, q, source)
	q:unlock()
	return ok and r or false
end

return setmetatable({}, _M)
