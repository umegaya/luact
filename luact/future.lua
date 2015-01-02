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
		local type,obj = event.wait(nil, self.cev, alarm)
		if obj == alarm then
			exception.raise('actor_timeout')
		end
	end
	-- type, eventobject, true or false, return values...
	if self.ret[3] then
		-- return values contains one more boolean on the top. why?
		-- => 
		-- return values... == arg1, arg2, ... 
		return unpack(self.ret, 4) -- return only return value
	else
		-- return values... == exception
		error(self.ret[4])
	end
end
function future_index:get(timeout)
	return pcall(self.unsafe_get, self, timeout)
end
function future_index:rawevent()
	return self.cev
end


local function check_completion(f)
	-- it must finish because at least f.ev emits timeout error.
	f.ret = {event.wait(nil, f.ev)}
end

-- ev has to be return value of actor:async_****
function _M.new(ev)
	local f = setmetatable({ev=ev}, future_mt)
	f.cev = tentacle(check_completion, f)
	return f
end

return _M
