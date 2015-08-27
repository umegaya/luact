local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_luact(3, nil, function ()
	local luact = require 'luact.init'
	local raft = require 'luact.cluster.raft'
	local actor = require 'luact.actor'
	local clock = require 'luact.clock'
	local uuid = require 'luact.uuid'
	local tools = require 'test.tools.cluster'
	local n_core = 3
	local leader_thread_id = 2
	local p = tools.create_latch('checker', 3)

	local arb, rft
	local initial_rs = tools.create_initial_replica_set(3)
	
	arb = actor.root_of(nil, luact.thread_id).arbiter('test_group', tools.new_fsm, {
		replica_set = initial_rs,
		debug_leader_uuid = actor.system_process_of(nil, leader_thread_id, luact.SYSTEM_PROCESS_RAFT_MANAGER),
	}, luact.thread_id)
	clock.sleep(2.5)
	rft = raft._find_body('test_group')
	if leader_thread_id == luact.thread_id then
		assert(uuid.equals(arb, rft:leader()), "this is only raft object to bootstrap, so should be leader")
	end
	p:wait(1)

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

