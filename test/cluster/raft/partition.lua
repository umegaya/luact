local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_local_cluster(5, 3, tools.new_fsm, function (arbiter, thread_id)
	logger.info('---------------------- start cluster ---------------------------')
	local luact = require 'luact.init'
	local actor = require 'luact.actor'
	local clock = require 'luact.clock'
	local uuid = require 'luact.uuid'
	local conn = require 'luact.conn'
	local tools = require 'test.tools.cluster'
	local event = require 'pulpo.event'
	local exception = require 'pulpo.exception'
	local tentacle = require 'pulpo.tentacle'
	tentacle.DEBUG2 = true
	local group1, group2 = { 1, 3 }, { 2, 4, 5 }
	local function is_group1(thread_id)
		for _,thid in ipairs(group1) do
			if thid == thread_id then return true end
		end
		return false
	end
	local initial_leader_thread_id = 3
	local p = tools.create_latch('checker', 5)
	-- start network partition
	if arbiter:is_leader() then
		clock.sleep(1.0) -- wait for last commit (replica_set) propagate to all node
	end
	local initial_leader = actor.root_of(nil, initial_leader_thread_id).arbiter('test_group')
	local orig_get_by_tid = conn.get_by_thread_id
	local g1 = is_group1(thread_id)
	local partitioned
	local black_hole = setmetatable({}, {
		__index = {
			dispatch = function (self, t)
				assert(partitioned, "should not be called blackhole dispatch unless partitioned")
				if bit.band(t.flag, actor.prefixes.notify_) ~= 0 then
					return
				elseif bit.band(t.flag, actor.prefixes.async_) ~= 0 then
					return tentacle(function (bt)
						clock.sleep(5)
						exception.raise('actor_timeout', t.id, bt)
					end, debug.traceback())
				else
					clock.sleep(5)
					exception.raise('actor_timeout', t.id, debug.traceback())
				end
			end,
		}
	})
	partitioned = true
	function conn.get_by_thread_id(tid)
		if g1 ~= is_group1(tid) then
			-- logger.info('getbytid:black hole', thread_id, '=>', tid)
			return black_hole
		else
			-- logger.info('getbytid:normal', thread_id, '=>', tid)
			return orig_get_by_tid(tid)
		end
	end
	logger.info('start network partition')
	-- wait for partitioned nodes got timeout...
	p:wait(1)
	logger.info('============================= goto phase 2')
	-- check group1 does not have any leader and group2 has still same leader.
	local arb, l
	local ok, r
	local evs = {}
	if is_group1(thread_id) then
		for _,tid in ipairs(group1) do
			arb = actor.root_of(nil, tid).arbiter('test_group')
			l = arb:leader()
			assert(uuid.equals(initial_leader, l), "group1: leader should not change:"..tostring(initial_leader).."|"..tostring(l))			
		end
		-- confirm any operation end in failure
		ok, r = arbiter:propose({{'hoge', 'fuga'}}, 1)
		assert((not ok) and r:is('actor_timeout'), "propose should fail because of not enough quorum")
		ok, r = arbiter:add_replica_set({arbiter}, 1)
		assert(not ok and r:is('actor_timeout'), "add_replica_set should fail because of not enough quorum")
		ok, r = arbiter:remove_replica_set({arbiter}, 1)
		assert(not ok and r:is('actor_timeout'), "remove_replica_set should fail because of not enough quorum")
	else
		local cnt = 0
		local l 
		while true do
			l = arbiter:leader()
			-- wait for different leader elected
			if (uuid.valid(l) and (not uuid.equals(initial_leader, l))) or (cnt > 20) then
				break
			end
			clock.sleep(1.0)
			cnt = cnt + 1
		end
		assert(cnt <= 20, "group2: should have valid leader:"..tostring(l).."|"..tostring(initial_leader))
		-- confirm able to write
		for i = thread_id * 10 + 1, thread_id * 10 + 10 do
			table.insert(evs, arbiter:async_propose({{i, i * 2}}))
		end
		event.join(clock.alarm(5), unpack(evs))
		arbiter:probe(function (rft)
			local fsm = rft.state.fsm
			for i = 21, 30 do
				assert(fsm[i] == i * 2, "logs should be applied (include partitioned term)")
			end
			for i = 41, 60 do
				assert(fsm[i] == i * 2, "logs should be applied (include partitioned term)")
			end
		end)
	end
	logger.info('============================= before goto phase 3')
	p:wait(2)
	logger.info('============================= goto phase 3')
	-- heal network partition
	conn.get_by_thread_id = orig_get_by_tid
	partitioned = false
	-- check group1 can write again and evantually group2's logs are replicated
	if is_group1(thread_id) then
		clock.sleep(10.0) -- wait for new leader overwrite status of group1
		assert(not uuid.equals(arbiter:leader(), initial_leader), "leader should change")
		-- confirm able to write
		for i = thread_id * 10 + 1, thread_id * 10 + 10 do
			table.insert(evs, arbiter:async_propose({{i, i * 2}}))
		end
		logger.info('============================== b4 probe fsm')
		local ret = event.join(clock.alarm(5), unpack(evs))
		assert(ret[#ret][1] ~= 'timeout', "operation should not timeout")
		clock.sleep(1.0) -- here, assure to apply logs to this node. 
		logger.info('============================== probe fsm')
		local ok, r = arbiter:probe(function (rft)
			local fsm = rft.state.fsm
			assert(not fsm.hoge, "logs written in partition term should not be applied")
			for i = 11, 60 do
				assert(fsm[i] == i * 2, "logs should be applied:"..tostring(fsm[i]).."|"..(i * 2))
			end
		end)
		assert(ok, "fsm apply end in failure:"..tostring(r))
	end
	logger.info('============================== wait all threads finished')
	p:wait(3)
	logger.info('============================== success')
end)

return true

