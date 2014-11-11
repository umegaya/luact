local clock = require 'luact.clock'
math.randomseed(clock.get())
if _G.counter > 0 then
	error('random error simulation')
end
_G.counter = _G.counter + 1
logger.info('creation success!!', _G.counter)
return {
	value = 'fuga',
	hoge = function (t, v)
		return t.value..tostring(v)
	end,
	throw = function (t, msg)
		error('error simulation:'..msg)
	end,
}
