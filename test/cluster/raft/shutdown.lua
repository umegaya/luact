local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_local_cluster(5, 1, tools.new_fsm, function (arbiter, thread_id)
	local tools = require 'test.tools.cluster'
	local clock = require 'luact.clock'
	local uuid = require 'luact.uuid'
	local actor = require 'luact.actor'
	local p = tools.create_latch('checker', 5)

	-- wait for all thread start
	p:wait(1)
	if thread_id == 1 then
		-- leader try to remove thread 4
		local arb = actor.root_of(nil, 4).arbiter('test_group')
		arbiter:remove_replica_set(arb)
	end
	-- wait for replica set change
	p:wait(2)
	if thread_id == 4 then
		-- arbiter is died, so any action to it fails
		local ok, r = pcall(arbiter.propose, arbiter, {{'fuga', 'hoge'}})
		assert((not ok) and r:is('raft_invalid'), "raft object in removed node should die")
	else
		local replicas = arbiter:replica_set()
		-- check replica set size is 4
		assert(#replicas == 4, "replica set size should be 4:"..tostring(#replicas))
		-- and thread 1, 2, 3, 5 is present
		for _, replica in ipairs(replicas) do
			local tid = uuid.thread_id(replica)
			local found 
			for _, id in ipairs({1,2,3,5}) do
				if tid == id then
					found = true
					break
				end
			end
			assert(found, "remained replica set should be predictable:"..tostring(replica))
		end
	end
	p:wait(3)
	logger.info('success')
end)

return true
