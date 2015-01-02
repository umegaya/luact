local pulpo = require 'pulpo.init'
local event = require 'pulpo.event'
local exception = require 'pulpo.exception'
local _M = (require 'pulpo.package').module('luact.defer.clock_c')
local clock = pulpo.evloop.clock.new(0.01, 10) -- res 10ms, max 10sec
local medium_clock = pulpo.evloop.clock.new(1, 60) -- res 1sec, max 60sec
local long_clock = pulpo.evloop.clock.new(60, 3600) -- res 1min, max 60min

local function get_clock(sec)
	if sec < 10 then
		return clock
	elseif sec < 60 then
		return medium_clock
	elseif sec < 3600 then
		return long_clock
	else
		exception.raise('invalid', 'duration', sec)
	end
end

function _M.sleep(sec)
	get_clock(sec):sleep(sec)
end
-- returns clock in sec (as double precision)
function _M.get()
	return pulpo.util.clock()
end

function _M.timer(interval, proc, ...)
	pulpo.tentacle(function (intv, fn, ...)
		while true do
			fn(...)
			_M.sleep(intv)
		end
	end, interval, proc, ...)
end

function _M.ticker(interval)
	local ticker = event.new()
	_M.timer(interval or 1.0, function (ev)
		ev:emit('tick')
	end, ticker)
	return ticker
end

function _M.alarm(duration)
	return get_clock(duration):alarm(duration)
end

return _M
