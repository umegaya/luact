local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_luact(1, nil, function ()
	local luact = require 'luact.init'
	local range = require 'luact.cluster.dht.range'
	local fs = require 'pulpo.fs'
	local util = require 'pulpo.util'
	local leader_thread_id = 1
	local root_range

	-- modify arbiter message so that range can use special raft actor for debug
	local range_arbiters = {}
	local consistent_flag
	function luact.root.arbiter(id, func, rng)
		local a = range_arbiters[id]
		if not a then
			a = luact({
				read = function (self, log, consistent, timeout)
					consistent_flag = consistent
					return rng:apply(log)
				end,
				write = function (self, log, timeout)
					return rng:apply(log)
				end,
			})
			range_arbiters[id] = a
		end
		return a
	end

	fs.rmdir("/tmp/luact/range_test")
	range.initialize(nil, "/tmp/luact/range_test", { 
		n_replica = 1, -- allow single node quorum
		storage = "rocksdb",
		datadir = luact.DEFAULT_ROOT_DIR,
	})
	
	local key = "hoge"
	print('put test')
	range.find(key):put(key, "fuga")
	print('get test')
	assert(range.find(key):get(key) == "fuga" and (not consistent_flag), "same value as given to put should be returned")
	print('cas test1')
	assert(not range.find(key):cas(key, "gyaa", "guha"), "cas should fail if condition not met")
	print('cas test2')
	assert(range.find(key):cas(key, "fuga", "guha"), "cas should success if condition met")
	print('get test2')
	assert(range.find(key):get(key, true) == "guha" and consistent_flag, "result of get also should change")
	
end)