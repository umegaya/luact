local luact = require 'luact.init'

luact.start({
	cache_dir = "/tmp/luact",
	n_core = 1, exclusive = true,
}, function ()
	local luact = require 'luact.init'
	local a1 = luact {
		hoge = function (n)
		print('hoge: called', n)
			return 'hoge'..n
		end,
	}
	local a2 = luact.load "./test/tools/test_actor.lua"
	local a3 = luact.require "ffiex"

	assert(a1.hoge(2) == 'hoge2')
	assert(a2:fuga(3) == 4)
	a3.cdef[[
		#define FOO (1)
	]]
	assert(a3.defs.FOO == 1)
end)

return true
