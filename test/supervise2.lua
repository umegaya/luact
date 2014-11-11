local luact = require 'luact.init'
local sv = require 'luact.supervise'
sv.DEBUG = true

luact.start({
	cache_dir = "/tmp/luact",
	n_core = 1, exclusive = true,
}, function ()
	local luact = require 'luact.init'
	local clock = require 'luact.clock'
	_G.counter = 0
	local iter = 10
	local a, svr = luact.supervise("./test/tools/error_actor.lua", { maxr = iter * 2 })
	assert(a:hoge(6) == 'fuga6')
	local msg = 'ugghhhhh!!!'
	local ok, r = pcall(a.throw, a, msg)
	local count = math.random(1, math.floor(iter / 2))
	for i=1,iter,1 do
		print('==================', i, count, _G.counter)
		if i == count then
			_G.counter = 0
		end
		ok, r = pcall(a.hoge, a, 9)
		if not ok then
			assert(r:is('actor_temporary_fail'))
		else
			assert(r == 'fuga9')
			break
		end
	end
	luact.stop()
end)

return true
