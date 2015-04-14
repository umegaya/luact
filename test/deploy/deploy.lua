local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

os.execute('bash ./test/tools/commit.sh --reset')
tools.start_luact(1, {
	deploy = {
		method = "github.com",
		pull = function (self)
			print('skip pull')
		end,
		repository = "foo/test",
		branches = {
			"master",
		},
		diff_base = "./test/deploy/foo",
	}
}, function ()
	local luact = require 'luact.init'
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
	deploy.hook_commit({
		header = {
			["X-GitHub-Event"] = "push",
		},
		body = {
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
	}, {
		last_commit = "HEAD^1",
	})
	print(' ==== start wait')
	luact.clock.sleep(1.0)
	print(' ==== end wait')
	for i=1,n_act do
		assert(actors[i]:bar() == 'foo.baz ver3')
	end
end)

return true
