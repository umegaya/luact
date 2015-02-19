local luact = require 'luact.init'
local tools = require 'test.tools.cluster'
local fs = require 'pulpo.fs'

tools.start_luact(5, nil, function ()
	local luact = require 'luact.init'
	local tools = require 'test.tools.cluster'
	local dht = require 'luact.cluster.dht'
	local uuid = require 'luact.uuid'
	local range = require 'luact.cluster.dht.range'
	local fs = require 'pulpo.fs'
	local util = require 'pulpo.util'

	local latch = tools.create_latch('checker', 3)
	-- modify arbiter message so that range can use special raft actor for debug
	if luact.thread_id == 1 then
		dht.initialize(nil, {datadir = "/tmp/luact"})
	else
		dht.initialize(luact.machine_id, {datadir = "/tmp/luact"})
	end	
	latch:wait(1)

	-- same test as single range manager but using raft consensus 
	local rm = range.get_manager()
	local ids = { 1, 2, 3, 4, 5 }
	for i=0, rm.root_range.replica_available do
		local r = rm.root_range.replicas[i]
		local tid = uuid.thread_id(r)
		for j=1,#ids do
			if ids[j] == tid then
				table.remove(ids, j)
			end
		end
	end
	if luact.thread_id == 1 then
		for i=1,#ids do
			print('remain id', ids[i])
		end
	end
	local function basic_functional_test(tid, msg)
		if luact.thread_id == tid then
			logger.notice(msg, luact.thread_id)
			local key = "hoge"
			local hlc = rm.clock
			logger.info('put test')
			rm:find(key):put(key, "fuga", hlc:issue())
			logger.info('get test')
			assert(rm:find(key):get(key, hlc:issue()) == "fuga", "same value as given to put should be returned")
			logger.info('cas test1')
			assert(not rm:find(key):cas(key, "gyaa", "guha", hlc:issue()), "cas should fail if condition not met")
			logger.info('cas test2')
			assert(rm:find(key):cas(key, "fuga", "guha", hlc:issue()), "cas should success if condition met")
			logger.info('get test2')
			assert(rm:find(key):get(key, hlc:issue(), true) == "guha", "result of get also should change")
		end
	end
	basic_functional_test(ids[1], 'test when this thread not involved in range replica')
	latch:wait(2)
	local last_replica_tid = uuid.thread_id(rm.root_range.replicas[rm.root_range.replica_available - 1])
	basic_functional_test(last_replica_tid, 'test when this thread involved in range replica')
	latch:wait(3)
end)

return true
