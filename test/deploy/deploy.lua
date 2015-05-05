local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

os.execute('bash ./test/tools/commit.sh --reset')
tools.start_luact(1, {
	deploy = {
		method = "github.com",
		pull = function (self, last_commit)
			print('skip pull')
			self.last_commit = last_commit
		end,
		repository = "foo/test",
		branches = {
			"master",
		},
		diff_base = "./test/deploy/foo",
		secret = "uversecret",
	}
}, function ()
	local luact = require 'luact.init'
	local serde = require 'luact.serde'
	local json = serde[serde.kind.json]
	local deploy = require 'luact.deploy'
	local n_act = 200
	local actors = {}
	luact.listen('tcp://0.0.0.0:8080')
	for i=1,n_act do
		luact.register('/baz'..i, './test/deploy/foo/main.lua')
		actors[i] = luact.ref('tcp://127.0.0.1:8080/baz'..i)
		assert(actors[i]:bar() == 'foo.baz ver2')
	end
	os.execute('bash ./test/tools/commit.sh')
	local headers = {
		["X-GitHub-Event"] = "push",
	}
	local body_table = {
		repository = {
			full_name = "foo/test",
		},
		ref = "refs/heads/master",
		commits = {
			modified = {
				"bar/baz/init.lua",
			}
		}
	}
	local body = json:pack_to_string(body_table)
	local opts = {
		last_commit = "HEAD^1",
	}
	assert(not deploy.hook_commit("POST", headers, body, opts))
	headers["X-Hub-Signature"] = "fuga"
	assert(not deploy.hook_commit("POST", headers, body, opts))
	headers["X-Hub-Signature"] = "724212dac3e665b393d28f08ba592016b6259e0a9741b9d7e81d978f2210b1bc"
	assert(deploy.hook_commit("POST", headers, body, opts))
	print(' ==== start wait')
	luact.clock.sleep(4.0)
	print(' ==== end wait')
	for i=1,n_act do
		assert(actors[i]:bar() == 'foo.baz ver3')
	end
end)

return true
