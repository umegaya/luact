local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_local_cluster(3, 2, tools.new_fsm, function (arbiter, thread_id)
	local clock = require 'luact.clock'
	local event = require 'pulpo.event'
	local util = require 'pulpo.util'
	local thread = require 'pulpo.thread'
	local tentacle = require 'pulpo.tentacle'
	local memory = require 'pulpo.memory'
	local tools = require 'test.tools.cluster'
	--tentacle.DEBUG2 = true
	logger.info('---------------------- start cluster ---------------------------')
	if not arbiter:is_leader() then
		clock.sleep(2.0) -- wait leader stale and heartbeat timeout
	end
	local p = tools.create_latch('checker', 3)
	-- make 10 propose for each thread
	local evs = {}
	local sidx=(thread_id - 1) * 20
	for i=sidx+1,sidx+10 do
		table.insert(evs, 
			arbiter:async_propose({
				{i, {thread_id, i * (10 + thread_id)}}
			})
		)
	end
	logger.info('wait committed')
	local res = event.join(clock.alarm(10), unpack(evs))
	logger.report('commit finished', arbiter:leader())
	clock.sleep(1.0) -- if this node is not leader, reach here only means commit is finished at leader node.
	logger.report('last check', arbiter:leader())
	-- so wait one more second to ensure entries commit at this node too.
	assert(res[#res][1] ~= 'timeout', 'take too long time to commit')
	if arbiter:is_leader() then
		logger.report('leader: propose finish, go stale.')
		util.sleep(5.0) -- stale. next leader should decided.
		logger.report('leader: end stale, check result')
	end
	local commit_results = {}
	for i=1,#res do
		table.insert(commit_results, {unpack(res[i], 3)})
	end
	local ok, r = arbiter:probe(function (rft, tid, res)
		local fsm = rft.state.fsm
		local sidx=(tid - 1) * 20
		local start_idx_in_tid, end_idx_in_tid
		for i = sidx+1, sidx+20 do
			if i <= (sidx+10) then
				local result, err = unpack(res[i - sidx])
				if result then
					assert(fsm[i], "commit should be done:"..tostring(i).."|"..tostring(tid))
					local commit_tid, val = unpack(fsm[i])
					assert(val == i * (10 + commit_tid), "commit should be done")
				else
					-- propose may be discarded if propose is sent to the leader 
					-- before its stale propagate to other node by heartbeat timeout
					assert(err:is('actor_timeout'), 'only timeout error is permisible:'..tostring(err))
				end
			else
				assert(not fsm[i], "commit should not be done")
			end
		end
	end, thread_id, commit_results)
	assert(ok, "proposed log should be applied to fsm:"..tostring(r))
	logger.info('success')
	p:wait(1)
	logger.info('all thread finished')
end)

return true