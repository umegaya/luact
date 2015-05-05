local luact = require 'luact.init'

print('luact strt')
luact.start({
	datadir = "/tmp/luact",
	n_core = 1, exclusive = true,
	dht = {
		gossip_port = false,	
	}, 
}, function ()
	local luact = require 'luact.init'
	local a1 = luact {
		recv_nil = function (x, y)
			assert(x == nil and y == 3)
			return nil, 6
		end,
		hoge = function (n)
			return 'hoge'..n
		end,
		__actor_destroy__ = function (t)
			_G.g_destroy = true
			logger.report('destroy called')
		end,			
	}
	print('luact run')
	local a2 = luact.load "./test/tools/test_actor.lua"
	print('actors', a1, a2)

	assert(a1.hoge(2, nil, false) == 'hoge2')
	local x, y = a1.recv_nil(nil, 3)
	assert(x == nil and y == 6)
	assert(a2:fuga(3) == 4)
	luact.kill(a1, a2)
	assert(_G.g_destroy)
	local ok, r = pcall(a1.hoge, 2)
	assert(not ok and (r:is('actor_no_body')))
end)

return true
