local clock = require 'luact.clock'
local exception = require 'pulpo.exception'
return {
	num = 1,
	fuga = function (t, add)
		return t.num + add
	end,
	inc_num = function (t, err_at)
		if t.num == err_at then
			exception.raise('actor_error', 'random error simulation:'..t.num)
		end
		t.num = t.num + 1
		return t.num
	end,
	sleep = function (t, duration)
		clock.sleep(duration)
		return duration * 2
	end,
}
