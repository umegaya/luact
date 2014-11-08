local clock = require 'luact.clock'
return {
	num = 1,
	fuga = function (t, add)
		return t.num + add
	end,
	sleep = function (t, duration)
	print('sleep:dur=',duration)
		clock.sleep(duration)
		return duration * 2
	end,
}
