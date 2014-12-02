local pulpo = require 'pulpo.init'
local event = require 'pulpo.event'
local _M = (require 'pulpo.package').module('luact.defer.clock_c')
local clock = pulpo.evloop.clock.new(0.01, 10)

function _M.sleep(sec)
	clock:sleep(sec)
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
	_M.timer(1.0, function (ev)
		ev:emit('tick')
	end, ticker)
	return ticker
end

function _M.alarm(duration)
	return clock:alarm(duration)
end

return _M
