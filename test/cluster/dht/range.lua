local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_luact(1, nil, function ()
	local luact = require 'luact.init'
	local tools = require 'test.tools.cluster'
	local range = require 'luact.cluster.dht.range'
	local fs = require 'pulpo.fs'
	local util = require 'pulpo.util'
	local hlc = (require 'pulpo.lamport').debug_make_hlc
	local leader_thread_id = 1
	local root_range
	local rm

	-- modify arbiter message so that range can use special raft actor for debug
	local consistent_flag
	tools.use_dummy_arbiter(function ()
		consistent_flag = false
	end, function (actor, logs, timeout, dictatorial)
		if ffi.typeof('luact_dht_cmd_get_t*') == ffi.typeof(logs[1]) then
			print('consistent_flag set')
			consistent_flag = true
		end
	end)

	fs.rmdir("/tmp/luact/range_test")
	rm = range.get_manager(nil, "/tmp/luact/range_test", { 
		n_replica = 1, -- allow single node quorum
		storage = "rocksdb",
		datadir = luact.DEFAULT_ROOT_DIR,
		range_size_max = 64 * 1024 * 1024
	})
	
	local key = "hoge"
	print('put test')
	rm:find(key, #key):put(key, "fuga")
	print('get test')
	assert(rm:find(key, #key):get(key) == "fuga" and (not consistent_flag), "same value as given to put should be returned")
	print('cas test1')
	local res = rm:find(key, #key):cas(key, "gyaa", "guha")
	assert(not res, "cas should fail if condition not met")
	print('cas test2')
	assert(rm:find(key, #key):cas(key, "fuga", "guha"), "cas should success if condition met")
	print('get test2')
	assert(rm:find(key, #key):get(key, nil, true) == "guha" and consistent_flag, "result of get also should change")
	print('delete test')
	rm:find(key, #key):delete(key)
	assert(not rm:find(key, #key):get(key), "result of get shouldn't be found")
	
	
end)

return true