local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_luact(5, nil, function ()
	local luact = require 'luact.init'
	local tools = require 'test.tools.cluster'
	local test = tools.test_runner
	local dht = require 'luact.cluster.dht'
	local uuid = require 'luact.uuid'
	local key = require 'luact.cluster.dht.key'
	local range = require 'luact.cluster.dht.range'
	local txncoord = require 'luact.storage.txncoord'
	local fs = require 'pulpo.fs'
	local util = require 'pulpo.util'

	local rm
	local function init_dht(range_max_bytes)
		rm = tools.init_dht({
			datadir = "/tmp/luact/split_test", 
			n_replica = 3, 
			max_clock_skew = 0,
			range_size_max = range_max_bytes,
		})
	end

	-- stepdown_lock locks duplicate stepdown with non-performant way.
	-- actual range manager locks duplicate operation to range by using persistent queue on dht
	local function stepdown_lock(latch, proc)
		latch:set(2)
		proc()
		latch:set(3)
	end

	local function range_rebalance_with_concurrent_txn(id, concurrency)
		local p, p2 = tools.create_latch('checker:'..tostring(id), 5), tools.create_latch('checker2:'..tostring(id), 5)
		local kind = range.KIND_STATE
		local controller = { retry = 0, done = false, exec = 0, }
		local evs = {}
		p:wait(1)
		if concurrency > 0 then
			for i=1,concurrency do
				table.insert(evs, luact.tentacle(tools.start_test_writer, rm, controller, i, kind, bit.lshift(1, 7)))
			end
		end

		logger.info('--------------------- start rebalancing --------------------- ')

		local k, kl = range.NON_META_MIN_KEY:as_slice()
		local done 
		while true do
			local rng = rm:find_range(k, kl, kind)
			-- logger.info('rng = ', rng, kind)
			if rng and rng:is_lead_by_this_node() then
				if done then
					local stepdown = true
					local s = ""
					for i=1,5 do
						local pr = p:progress(i)
						s = s .. tostring(pr)
						if pr == 2 then
							stepdown = false -- some thread lock stepdown
						end
					end
					logger.info('current leader check stepdown', s, stepdown, rng.replica_available)
					if stepdown then
						if rng.replica_available ~= 3 then
							logger.info('range state invalid (replica number)', s, rng)
							os.exit(-1)
						end
						logger.info('stepdown leader', luact.thread_id)
						stepdown_lock(p, function ()
							rng:raft_body():stepdown(nil, 60)
						end)
					end
				else
					stepdown_lock(p, function ()
						luact.clock.sleep(1.0) -- wait for previous range moving finished
						-- leader try to rebalance itself
						local vacant_tids = {}
						for i=1,5 do
							local found
							for j=0,rng.replica_available-1 do
								local id = rng.replicas[j]
								-- find thread which does not assigned raft instance and have not been complete rebalance yet.
								if (uuid.thread_id(id) == i) then -- or (p:progress(i) >= 2) then
									found = true
									break
								end
							end
							if not found then
								table.insert(vacant_tids, i)
							end
						end
						local target_tid = vacant_tids[math.random(1, #vacant_tids)]
						logger.report('move range', rng, 'from', luact.thread_id, 'to', target_tid)
						rng:move_to(nil, target_tid)
						logger.report('--------------------- move range finished ---------------------', luact.thread_id)
						done = true
					end)
				end
			end
			local finish = true
			local state = ""
			for i=1,5 do
				state = state .. tostring(tonumber(p:progress(i)))
				if p:progress(i) < 2 then
					finish = false
				end
			end
			if finish then
				logger.warn('finished', luact.thread_id)
				break
			end
			if luact.thread_id == 1 then io.write('--------------------- progress:'..state..'\n') end
			luact.clock.sleep(0.5)
		end
		p:wait(4)

		controller.done = true
		if #evs > 0 then
			luact.event.join(nil, unpack(evs))
			logger.report('writer does', controller.exec, 'txn')
		end
		if controller.retry ~= 0 then
			assert(false, ("expected no retries splitting a range with concurrent writes, "..
				"as range splits do not cause conflicts; got %d"):format(controller.retry))
		end
		p:wait(5)

		local final_rng = rm:find(kind, k, kl)
		luact.clock.sleep(2) -- waiting for final replication done
		logger.warn('final_rng', final_rng)
		assert(final_rng.replica_available == 3)
		for i=0,final_rng.replica_available-1 do
			local id = final_rng.replicas[i]
			if uuid.owner_of(id) then
				p:set(7)
			end
		end
		p2:wait(1)
		local cnt = 0
		local s = ""
		for i=1,5 do
			local pr = p:progress(i)
			s = s .. tostring(pr)
			if pr > 6 then
				cnt = cnt + 1
			end
		end
		assert(cnt == 3, s)
		p2:wait(2)
	end

	-- TestRangeSplits does 5 consecutive rebalances
	test("TestRangeRebalances", function ()
		init_dht()
		range_rebalance_with_concurrent_txn(1, 0)
	end)

	--[[
	-- TestRangeSplitsWithConcurrentTxns does 5 consecutive rebalance while
	-- 10 concurrent coroutines are each running successive transactions
	-- composed of a random mix of puts.
	test("TestRangeRebalancesWithConcurrentTxns", function ()
		init_dht()
		range_rebalance_with_concurrent_txn(2, 10)
	end)
	]]
end)

return true
