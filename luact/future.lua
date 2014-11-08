local event = require 'pulpo.event'
local exception = require 'pulpo.exception'
local tentacle = require 'pulpo.tentacle'

local clock = require 'luact.clock'

local _M = {}

local future_index = {}
local future_mt = {
	__index = future_index
}
function future_index:finished()
	return self.ret
end
function future_index:unsafe_get(timeout)
	if not self:finished() then
		local alarm = timeout and clock.alarm(timeout) or nil
		local type,obj = event.select(nil, self.cev, alarm)
		if obj == alarm then
			exception.raise('actor_reply_timeout')
		end
	end
	-- type, eventobject, true or false, return values... or exception object
	if self.ret[3] then
		-- return values... == true, arg1, arg2, ... 
		return unpack(self.ret, 5) -- return only return value
	else
		-- return values... == false, exception
		error(self.ret[5])
	end
end
function future_index:get(timeout)
	return pcall(self.unsafe_get, self, timeout)
end


local function check_completion(f)
	-- it must finish because at least f.ev emits timeout error.
	f.ret = {event.select(nil, f.ev)}
end


function _M.new(ev)
	local f = setmetatable({ev=ev}, future_mt)
	f.cev = tentacle(check_completion, f)
	return f
end

return _M
