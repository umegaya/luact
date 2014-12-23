local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_local_cluster(3, 1, tools.new_fsm, function (arbiter, thread_id)
	local luact = require 'luact.init'
	local clock = require 'luact.clock'
	local uuid = require 'luact.uuid'
	local actor = require 'luact.actor'
	local event = require 'pulpo.event'
	local util = require 'pulpo.util'
	logger.info('---------------------- start cluster ---------------------------')
	local thread_id_leader_map = {}
	function luact.root.report(thread_id, leader_id)
		thread_id_leader_map[thread_id] = leader_id
	end
	if thread_id == 1 then
		arbiter:stepdown()
		clock.sleep(3)
		local count, leader_id = 0, nil
		for k,v in pairs(thread_id_leader_map) do
			count = count + 1
			if not leader_id then
				leader_id = v
			elseif not uuid.equals(leader_id, v) then
				assert(false, "leader_id should be consistent among all thread")
			end
		end
		assert(leader_id, "new leader should be decided")
	else
		while not arbiter:is_leader() do
			local l = arbiter:leader()
			if uuid.valid(l) and (not uuid.equals(l, actor.root_of(nil, 1))) then
				break
			end
			clock.sleep(0.1)
		end
		actor.root_of(nil, 1).report(thread_id, arbiter:leader())
	end
	logger.info('success')
	clock.sleep(2)
end)

return true
