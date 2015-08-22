local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_luact(3, nil, function ()
	local luact = require 'luact.init'
	local raft = require 'luact.cluster.raft'
	local actor = require 'luact.actor'
	local clock = require 'luact.clock'
	local uuid = require 'luact.uuid'
	local thread = require 'pulpo.thread'
	local pulpo = require 'pulpo.init'
	local tools = require 'test.tools.cluster'
	local n_core = 3
	local leader_thread_id = 2
	local p = tools.create_latch('checker', 3)

	local arb, rft
	
	if pulpo.thread_id == leader_thread_id then
		arb = actor.root_of(nil, pulpo.thread_id).arbiter('test_group', tools.new_fsm, {initial_node = true}, pulpo.thread_id)
		clock.sleep(2.5)
		rft = raft._find_body('test_group')
		assert(uuid.equals(arb, rft:leader()), "this is only raft object to bootstrap, so should be leader")
		logger.info('------------------- add another nodes as replica set ---------------------')
		local replica_set = {}
		for i=1,n_core do
			local replica = actor.root_of(nil, i).arbiter('test_group', tools.new_fsm, nil, i)
			assert(replica, "arbiter should be created")
			table.insert(replica_set, replica)
		end
		logger.info('------------------- call add_replica_set() ---------------------')
		rft:add_replica_set(replica_set)
		logger.info('------------------- finish add_replica_set() ---------------------')
		p:wait(1)
	else
		logger.info('------------------- wait for being added as replica set ---------------------')
		p:wait(1)
		arb = actor.root_of(nil, pulpo.thread_id).arbiter('test_group')
		rft = raft._find_body('test_group')
	end
	local rs = rft:replica_set()
	assert(#rs == n_core, "# of replica_set should be "..n_core..":"..#rs)
	local found
	for i=1,n_core do
		if uuid.equals(rs[i], arb) then
			found = true
			break
		end
	end
	assert(found, "each thread's uuid should be included in replica set:"..tostring(arb))
	logger.info('success')
	p:wait(2)
end)

return true

