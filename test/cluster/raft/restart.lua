local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_local_cluster(5, 1, tools.new_fsm, function (arbiter, thread_id)
	local tools = require 'test.tools.cluster'
	local clock = require 'luact.clock'
	local uuid = require 'luact.uuid'
	local actor = require 'luact.actor'
	local event = require 'pulpo.event'
	local tools = require 'test.tools.cluster'
	local p = tools.create_latch('checker', 5)

	-- wait for all thread start
	p:wait(1)
	if thread_id == 1 then
	logger.info('-------------------- remove raft instance -----------------------------')
		-- remove thread 4 
		arbiter:remove_replica_set(actor.root_of(nil, 4).arbiter('test_group'))
	end
	p:wait(2)
	if thread_id == 4 then
		-- arbiter is died, so any action to it fails
		local ok, r = pcall(arbiter.propose, arbiter, {{'fuga', 'hoge'}})
		assert((not ok) and r:is('actor_no_body'), "raft object in removed node should die")
	else
		-- each thread write enough number of record which causes snapshotting
		-- and wait for completion
		local evs = {}
		for i=thread_id * 10 + 1, thread_id * 10 + 10 do
			table.insert(evs, arbiter:async_propose({{i, i + 123}}))
		end
		event.join(clock.alarm(5), unpack(evs))
		clock.sleep(1.0)
	end
	p:wait(3)
	if thread_id == 1 then
	logger.info('-------------------- re-initialize raft instance -----------------------------')
		-- add thread 4
		local arb = actor.root_of(nil, 4).arbiter('test_group', tools.new_fsm, nil, 4)
		arbiter:add_replica_set(arb)
	end
	p:wait(4)
	-- wait replication to thread 4 finished
	clock.sleep(0.1)
	if thread_id == 4 then
		arbiter = actor.root_of(nil, 4).arbiter('test_group')
		assert(arbiter and uuid.valid(arbiter), "thread 4's arbiter should be recovered")
	end
	local replica_set = arbiter:replica_set()
	assert(#replica_set == 5, "replica set should be size 5")

	-- check all thread (including thread 4) 
	local ok, r = arbiter:probe(function (r)
		local fsm = r.state.fsm
		for i=11,40 do
			assert(fsm[i] == i + 123, "all logs should be applied:"..tostring(i).."|"..tostring(fsm[i]))
		end
		for i=51,60 do
			assert(fsm[i] == i + 123, "all logs should be applied:"..tostring(i).."|"..tostring(fsm[i]))
		end
	end)
	assert(ok, "fsm apply fails:"..tostring(r))
	p:wait(5)	
	logger.info('success')
end)

return true
