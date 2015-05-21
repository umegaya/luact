local exlib = require 'luact.exlib'
local pulpo = require 'pulpo.init'
local opts = {
	datadir = '/tmp/luact'
}
pulpo.initialize(opts)
pulpo.run({
	n_core = 1,
	exclusive = true,
}, function ()
	os.execute('bash ./test/tools/commit.sh --reset')

	local luact = require 'luact.init'
	local mod = require 'luact.module'
	local pulpo = require 'pulpo.init'
	local util = require 'pulpo.util'
	local proc = require 'pulpo.io.process'
	local clock = pulpo.evloop.clock.new(0.05, 10)
	proc.initialize(function (dur)
		return clock:alarm(dur)
	end)
	luact.process = pulpo.evloop.io.process
	require('luact.defer.module_c')
	
	local foomain = require 'test.deploy.foo.main'

	local loaded = {
		["6dd51173d85ee4c25893226e6d8e5435fa739202"] = {"bar"},
		["01d898ef94899c6312c4ac3b8bc455e7a997335a"] = {"baz"},
		["a56773fa203b8299ffb15a982066a83bec36a9ed"] = {"baz"},
	}

	for k,v in pairs(loaded) do
		assert(mod.loaded[k], "submodule should exists:"..k)
		for i=1,#v do
			assert(mod.loaded[k][v[i]], "entry should exists:"..v[i])
		end
	end

	os.execute('bash ./test/tools/commit.sh')
	local list = mod.diff_recursive('4f4dc0f06330f77e1a7f55db1249d7502ee51f15', 'HEAD', './test/deploy/foo')
	assert(#list == 1)
	assert(list[1] == './test/deploy/foo/bar/baz/init.lua')
	os.execute('bash ./test/tools/commit.sh --reset')
	print('success')
end)

return true
