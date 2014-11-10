local clock = require 'luact.clock'
return {
	num = 1,
	fuga = function (t, add)
		return t.num + add
	end,
	inc_num = function (t, err_at)
		if t.num == err_at then
			error('random error simulation')
		end
		t.num = t.num + 1
		return t.num
	end,
	sleep = function (t, duration)
		clock.sleep(duration)
		return duration * 2
	end,
}
