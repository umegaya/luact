local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_local_cluster(5, 3, tools.new_fsm, function (arbiter, thread_id)
	local luact = require 'luact.init'
	local clock = require 'luact.clock'
	local uuid = require 'luact.uuid'
	local actor = require 'luact.actor'
	local event = require 'pulpo.event'
	local util = require 'pulpo.util'
	local tools = require 'test.tools.cluster'
	local p = tools.create_latch('checker', 5)
	logger.info('---------------------- start cluster ---------------------------')
	local first_leader = actor.system_process_of(nil, 3, luact.SYSTEM_PROCESS_RAFT_MANAGER)
	if thread_id == 3 then
		assert(arbiter:stepdown())
	else
		local ok, r = pcall(arbiter.stepdown, arbiter)
		assert(not ok, "only leader can step down")
	end
	p:wait(1)
	local l = ffi.new('luact_uuid_t')	
	while true do
		ffi.copy(l, arbiter:leader(), ffi.sizeof('luact_uuid_t'))
		if uuid.valid(l) then 
			assert(not uuid.equals(l, first_leader))
			break
		end
		clock.sleep(0.1)
	end
	p:wait(2)
	local rs
	if uuid.owner_of(l) then
		logger.info('---------------------- leader remove itself ---------------------------', l, uuid.owner_of(l))
		arbiter:remove_replica_set({l})
		rs = arbiter:replica_set()
	else
		while true do
			rs = arbiter:replica_set()
			if #rs == 4 then
				break
			end
			clock.sleep(0.1)
		end
	end
	p:wait(3)
	local ll = arbiter:leader()
	assert(not uuid.equals(ll, l))
	assert(#rs == 4) -- only 1 replica removed
	for i=1,#rs do
		if uuid.equals(rs[i], l) then
			assert('leader should remove from replicaset')
		end
	end
	logger.info('success')
	p:wait(4)
end)

return true
