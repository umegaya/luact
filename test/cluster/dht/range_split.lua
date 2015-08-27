local luact = require 'luact.init'
local tools = require 'test.tools.cluster'
local fs = require 'pulpo.fs'

tools.start_luact(1, nil, function ()
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
			n_replica = 1, 
			max_clock_skew = 0,
			range_size_max = range_max_bytes,
		}, true)
	end

	local function range_split_with_concurrent_txn(concurrency)
		-- Start up the concurrent goroutines which run transactions.
		local kind = range.KIND_STATE
		local controller = { retry = 0, done = false, exec = 0, }
		local evs = {}
		if concurrency > 0 then
			for i=1,concurrency do
				table.insert(evs, luact.tentacle(tools.start_test_writer, rm, controller, i, kind, bit.lshift(1, 7)))
			end
		end
		local k, kl = range.META2_MIN_KEY:as_slice()
		local ek, ekl = range.META2_MAX_KEY:as_slice()
		local meta2_ranges = rm:scan(range.KIND_META2, k, kl, ek, ekl, 0)
		-- meta2 should has KIND_VID, KIND_STATE
		assert(#meta2_ranges == 2, "meta2 should have KIND_VID, KIND_STATE")
		-- Set five split keys, about evenly spaced along the range of random keys.
		local split_keys = {"G", "R", "a", "l", "s"}
		-- Execute the consecutive splits.
		for _, k in ipairs(split_keys) do
			rm:find(kind, k, #k):split(k)
		end

		controller.done = true
		if #evs > 0 then
			luact.event.join(nil, unpack(evs))
			logger.report('writer does', controller.exec, 'txn')
		end

		if controller.retry ~= 0 then
			assert(false, ("expected no retries splitting a range with concurrent writes, "..
				"as range splits do not cause conflicts; got %d"):format(controller.retry))
		end

		meta2_ranges = rm:scan(range.KIND_META2, k, kl, ek, ekl, 0)
		assert(#meta2_ranges == 2 + #split_keys, "KIND_STATE devided into 1 + #split_keys")
		table.insert(split_keys, ffi.string(range.NON_META_MAX_KEY:as_slice()))
		for i=1,#meta2_ranges do
			local mr = meta2_ranges[i]
			-- each entry us [k, kl, v, vl, ts], and v should be the range object
			local r = ffi.cast('luact_dht_range_t *', mr[3])
			local entry_key = ffi.string(mr[1], mr[2])
			local end_key = ffi.string(r.end_key:as_slice())
			if r.kind == range.KIND_STATE then
				local found 
				for j=1,#split_keys do
					local spk = split_keys[j]
					local metakey = ffi.string(key.make_metakey(kind, spk, #spk))
					if metakey == entry_key and spk == end_key then
						found = true
						break
					end
				end
				if not found then
					logger.info('not found', end_key, table.concat(split_keys), r)
					assert(false)
				end
			end
		end
	end

	local range_max_bytes = bit.lshift(1, 18)
	local function range_write_pressure()
		local controller = { retry = 0, done = false, exec = 0, }
		local kind = range.KIND_STATE
		local k, kl = range.META2_MIN_KEY:as_slice()
		local ek, ekl = range.META2_MAX_KEY:as_slice()
		local vlen = bit.lshift(1, 15)
		local est_splits = 5

		-- Start test writer write about a 32K/key so there aren't too many writes necessary to split 64K range.
		local tev = luact.tentacle(tools.start_test_writer, rm, controller, 0, kind, vlen)

		-- Check that we split 5 times in allotted time.
		local meta2_ranges
		assert(tools.is_true_within(function ()
			meta2_ranges = rm:scan(range.KIND_META2, k, kl, ek, ekl, 0)
			return #meta2_ranges >= (est_splits + 2)
		end, 6.0), "actual split times not enough for estimated:"..#meta2_ranges.."/"..est_splits)

		controller.done = true
		luact.event.join(nil, tev)
		logger.report('writer does', controller.exec, 'txn')
		
		-- This write pressure test often causes splits while resolve
		-- intents are in flight, causing them to fail with range key
		-- mismatch errors. However, LocalSender should retry in these
		-- cases. Check here via MVCC scan that there are no dangling write
		-- intents. We do this using an IsTrueWithin construct to account
		-- for timing of finishing the test writer and a possibly-ongoing
		-- asynchronous split.
		assert(tools.is_true_within(function ()
			return pcall(rm.scan, rm, kind, k, kl, ek, ekl, 0, nil, true)
		end, 0.5), "finishing concurrent write fails within estimated time")
	end

	-- TestRangeSplits does 5 consecutive splits
	test("TestRangeSplits", function ()
		init_dht()
		range_split_with_concurrent_txn(0)
	end)

	-- TestRangeSplitsWithConcurrentTxns does 5 consecutive splits while
	-- 10 concurrent coroutines are each running successive transactions
	-- composed of a random mix of puts.
	test("TestRangeSplitsWithConcurrentTxns", function ()
		init_dht()
		range_split_with_concurrent_txn(10)
	end)

	-- TestRangeSplitsWithWritePressure sets the zone config max bytes for
	-- a range to 256K and writes data until there are five ranges.
	test("TestRangeSplitsWithWritePressure", function ()
		init_dht(range_max_bytes)
		range_write_pressure()
	end)

	-- TestRangeSplitsWithSameKeyTwice check that second range split
	-- on the same splitKey should not cause infinite retry loop.
	test("TestRangeSplitsWithSameKeyTwice", function ()
		local kind = range.KIND_STATE
		local split_key = "aa"
		logger.info("starting split at key", split_key)
		rm:split(kind, split_key)
		logger.info("split key first time complete", split_key)
		local ok, r = pcall(rm.split, rm, kind, split_key)
		assert(not ok and r:is('invalid'))
	end)
end)

return true
