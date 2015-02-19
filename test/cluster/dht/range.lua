local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_luact(1, nil, function ()
	local luact = require 'luact.init'
	local range = require 'luact.cluster.dht.range'
	local fs = require 'pulpo.fs'
	local util = require 'pulpo.util'
	local hlc = (require 'pulpo.lamport').debug_make_hlc
	local leader_thread_id = 1
	local root_range
	local rm

	-- modify arbiter message so that range can use special raft actor for debug
	local range_arbiters = {}
	local consistent_flag
	function luact.root.arbiter(id, func, opts, rng)
		local rm = (require 'luact.cluster.dht.range').get_manager()
		rng = rm:create_fsm_for_arbiter(rng)
		local storage = rm.kv_groups[rng.kind]
		local a = range_arbiters[id]
		if not a then
			a = luact({
				read = function (self, key, timeout)
					consistent_flag = false
					return rng:exec_get(storage, key, #key)
				end,
				write = function (self, logs, timeout, dictatorial)
					if ffi.typeof('luact_dht_cmd_get_t*') == ffi.typeof(logs[1]) then
						consistent_flag = true
					end
					logger.info('write', logs, timeout, dictatorial)
					return rng:apply(logs[1])
				end,
			})
			range_arbiters[id] = a
			rng:debug_add_replica(a)
		end
		return a
	end

	fs.rmdir("/tmp/luact/range_test")
	rm = range.get_manager(nil, "/tmp/luact/range_test", { 
		n_replica = 1, -- allow single node quorum
		storage = "rocksdb",
		datadir = luact.DEFAULT_ROOT_DIR,
	})
	
	local key = "hoge"
	print('put test', rm)
	rm:find(key):put(key, "fuga", hlc(1))
	print('get test')
	assert(rm:find(key):get(key, hlc(1)) == "fuga" and (not consistent_flag), "same value as given to put should be returned")
	print('cas test1')
	local res = rm:find(key):cas(key, "gyaa", "guha", hlc(1))
	print('cas test1:', res)
	assert(not res, "cas should fail if condition not met")
	print('cas test2')
	assert(rm:find(key):cas(key, "fuga", "guha", hlc(1)), "cas should success if condition met")
	print('get test2')
	assert(rm:find(key):get(key, hlc(1), true) == "guha" and consistent_flag, "result of get also should change")
	
end)

return true