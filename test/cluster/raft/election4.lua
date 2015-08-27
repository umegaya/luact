local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_luact(5, nil, function ()
	local luact = require 'luact.init'
	local raft = require 'luact.cluster.raft'
	local clock = require 'luact.clock'
	local uuid = require 'luact.uuid'
	local actor = require 'luact.actor'
	local event = require 'pulpo.event'
	local util = require 'pulpo.util'
	local tools = require 'test.tools.cluster'
	local p = tools.create_latch('checker', 5)
	logger.info('---------------------- start cluster ---------------------------')
	local raft_members = {1, 3, 5}
	local raft_manager_actors = {}
	local is_raft_member
	for i=1,#raft_members do
		if raft_members[i] == luact.thread_id then
			is_raft_member = true
		end
		table.insert(raft_manager_actors, actor.system_process_of(nil, raft_members[i], luact.SYSTEM_PROCESS_RAFT_MANAGER))
	end
	if is_raft_member then
		luact.root.arbiter('test_group', tools.new_fsm, {
			replica_set = raft_manager_actors,
		}, luact.thread_id)
		p:wait(1)
		clock.sleep(2.5)
		local arbiter = raft._find_body('test_group')
		local found
		for i=1,#raft_manager_actors do
			if uuid.equals(arbiter:leader(), raft_manager_actors[i]) then
				found = true
			end
		end
		assert(found, "independently started raft group cannot find leader within reasonable time")
	else
		assert(not raft._find_body('test_group'))
	end
	logger.info('success')
	p:wait(2)
end)

return true
