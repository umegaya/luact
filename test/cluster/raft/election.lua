local luact = require 'luact.init'

local function start_cluster(n_core, fn)
	luact.start({
		cache_dir = "/tmp/luact",
		n_core = n_core, exclusive = true,
	}, fn)
end
local function test()
	local luact = require 'luact.init'
	local actor = require 'luact.actor'
	local clock = require 'luact.clock'
	local uuid = require 'luact.uuid'
	local pulpo = require 'pulpo.init'
	local function new_fsm(thread_id)
		return setmetatable({}, {
			__index = {
				metadata = function (self)
					return {'this', 'is', meta = 'data', ['for'] = thread_id}
				end,
				snapshot = function (self, sr, rb)
					sr:pack(rb, self)
				end,
				restore = function (self, sr, rb)
					local obj = sr:unpack(rb)
					for k,v in pairs(obj) do
						self[k] = v
					end
				end,
				apply = function (self, data)
					self[data[1]] = data[2]
				end,
				attach = function (self)
				end,
				detach = function (self)
				end,
			}
		})
	end
	local ok,r = xpcall(function ()
		local fsm = new_fsm()
		local replica_set = {}
		local arb = actor.root_of(nil, pulpo.thread_id).arbiter('test_group', new_fsm, nil, pulpo.thread_id)
		clock.sleep(3)
		print('leader:', arb:leader())
		assert(uuid.valid(arb:leader()), "leader should be elected")
	end, function (e)
		logger.error('err', e)
	end)

	luact.stop()
end

print('-- 1 core test')
start_cluster(1, test)
--print('-- 5 core test')
--start_cluster(5, test)

return true

