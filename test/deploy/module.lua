local exlib = require 'luact.exlib'
local pulpo = require 'pulpo.init'
local opts = {
	datadir = './tmp'
}
pulpo.initialize(opts)
pulpo.run({
	n_core = 1,
	exclusive = true,
}, function ()
	os.execute('bash ./test/tools/commit.sh --reset')

	local mod = require 'luact.module'
	local util = require 'pulpo.util'

	local foomain = require 'test.deploy.foo.main'

	local loaded = {
		["206f5eb40817133469bddad419dd87323890e305"] = {"bar"},
		["01d898ef94899c6312c4ac3b8bc455e7a997335a"] = {"baz"},
		["2c3ae38ede0b2ba1cd0df9a837afe0f7cfc23946"] = {"baz"},
	}

	for k,v in pairs(loaded) do
		assert(mod.loaded[k], "submodule should exists:"..k)
		for i=1,#v do
			assert(mod.loaded[k][v[i]], "entry should exists:"..v[i])
		end
	end

	os.execute('bash ./test/tools/commit.sh')
	local list = mod.diff_recursive('HEAD^1', 'HEAD', './test/deploy/foo')
	assert(#list == 1)
	assert(list[1] == './test/deploy/foo/bar/baz/init.lua')
	os.execute('bash ./test/tools/commit.sh --reset')
	print('success')
end)

return true
