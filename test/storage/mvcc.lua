local luact = require 'luact.init'
local tools = require 'test.tools.cluster'
local fs = require 'pulpo.fs'

tools.start_luact(1, nil, function ()

local mvcc = require 'luact.storage.mvcc.rocksdb'
local key = require 'luact.cluster.dht.key'
local txncoord = require 'luact.storage.txncoord'
local actor = require 'luact.actor'
local lamport = require 'pulpo.lamport'
local util = require 'pulpo.util'
local fs = require 'pulpo.fs'
local exception = require 'pulpo.exception'

txncoord.initialize({
	find = function ()
		return nil
	end,
	clock = lamport.new_hlc(),
})


-- Constants for system-reserved keys in the KV map.
local test_key1     = "db1"
local test_key2     = "db2"
local test_key3     = "db3"
local test_key4     = "db4"
local txn1         	= txncoord.debug_make_txn({
	coord = actor.root_of(nil, 1),
	start_at = lamport.debug_make_hlc(0, 1),
	timestamp = lamport.debug_make_hlc(0, 1),  
})
local txn1_commit  	= txncoord.debug_make_txn({
	coord = actor.root_of(nil, 1),
	start_at = lamport.debug_make_hlc(0, 1),
	timestamp = lamport.debug_make_hlc(0, 1),  
	status = txncoord.STATUS_COMMITTED,
})
local txn1_abort  	= txncoord.debug_make_txn({
	coord = actor.root_of(nil, 1),
	start_at = lamport.debug_make_hlc(0, 1),
	timestamp = lamport.debug_make_hlc(0, 1),  
	status = txncoord.STATUS_ABORTED,
})
local txn1_1		= txncoord.debug_make_txn({
	coord = actor.root_of(nil, 1),
	start_at = lamport.debug_make_hlc(0, 1),
	timestamp = lamport.debug_make_hlc(0, 1),  
	n_retry = 1
})
local txn1_1_commit	= txncoord.debug_make_txn({
	coord = actor.root_of(nil, 1),
	start_at = lamport.debug_make_hlc(0, 1),
	timestamp = lamport.debug_make_hlc(0, 1),  
	status = txncoord.STATUS_COMMITTED,
	n_retry = 1
})
local txn2         	= txncoord.debug_make_txn({
	coord = actor.root_of(nil, 2),
	start_at = lamport.debug_make_hlc(0, 1),
	timestamp = lamport.debug_make_hlc(0, 1),  
})
local txn2_commit  	= txncoord.debug_make_txn({
	coord = actor.root_of(nil, 2),
	start_at = lamport.debug_make_hlc(0, 1),
	timestamp = lamport.debug_make_hlc(0, 1),  
	status = txncoord.STATUS_COMMITTED,
})
local value1       = "testValue1"
local value2       = "testValue2"
local value3       = "testValue3"
local value4       = "testValue4"
local valueEmpty   = ""

-- create/destroy test database
local function create_db()
	fs.rmdir('/tmp/luact/txntest')
	return mvcc.open('/tmp/luact/txntest'), ffi.new('luact_mvcc_stats_t')
end
local function destroy_db(db)
	mvcc.close(db)
end

-- makeTxn creates a new transaction using the specified base
-- txn and timestamp.
local function maketxn(txn, ts)
	return txn:clone({
		timestamp = ts,
		max_ts = ts,
	})
end
local function new_txn(opts)
	return txncoord.debug_make_txn(opts)
end

-- makeTS creates a new hybrid logical timestamp.
local function makets(logical, msec)
	return lamport.debug_make_hlc(logical, msec)
end

function encode_size(k, kl, ts)
	return #mvcc.make_key(k, kl or #k, ts)
end

function verify_stats(exp, st)
	-- print(st.bytes_val, st.bytes_key, exp.bytes_val, exp.bytes_key, st, exp)
	return st.bytes_val == exp.bytes_val and st.bytes_key == exp.bytes_key
end


local function test_err_handle(e)
	return exception.new_with_bt('runtime', debug.traceback(), e)
end
local function test(name, proc, ctor, dtor)
	local r 
	if ctor then
		r = {xpcall(ctor, test_err_handle)}
		if not r[1] then
			error(r[2])
		end
	end
	local args
	if r then
		args = util.copy_table(r)
		r = {xpcall(proc, test_err_handle, unpack(r, 2))}
	else
		r = {xpcall(proc, test_err_handle)}
	end
	if not r[1] then
		error(r[2])
	end
	if dtor then
		if args then
			r = {xpcall(dtor, test_err_handle, unpack(args, 2))}
		else
			r = {xpcall(dtor, test_err_handle)}
		end
		if not r[1] then
			error(r[2])
		end
	end
	logger.notice(name, 'success')
end

-- [[
-- Verify the sort ordering of successive keys with metadata and
-- versioned values. In particular, the following sequence of keys /
-- versions:
--
-- a
-- a<t=0>
-- a<t=1>
-- a<t=max>
-- a\x00
-- a\x00<t=0>
-- a\x00<t=1>
-- a\x00<t=max>
test("TestMVCCKeys", function ()
	local z = "\0"
	local a = "a"
	local a0 = "a\0"
	local aa = "aa"
	local b = "b"
	local keys = {
		mvcc.make_key(z, #z),
		mvcc.make_key(z, #z, makets(0, 0)),
		mvcc.make_key(z, #z, makets(1, 0)),
		mvcc.make_key(z, #z, makets(0, 1)),
		mvcc.make_key(z, #z, makets(0, lamport.MAX_HLC_WALLTIME)),
		mvcc.make_key(a, #a),
		mvcc.make_key(a, #a, makets(0, 0)),
		mvcc.make_key(a, #a, makets(0, 1)),
		mvcc.make_key(a, #a, makets(0, lamport.MAX_HLC_WALLTIME)),
		mvcc.make_key(a0, #a0),
		mvcc.make_key(a0, #a0, makets(0, 0)),
		mvcc.make_key(a0, #a0, makets(0, lamport.MAX_HLC_WALLTIME)),
		mvcc.make_key(aa, #aa),
		mvcc.make_key(b, #b),
	}
	local copied = util.copy_table(keys)
	for k,v in pairs(copied) do
	--	mvcc.dump_key(v)
	end
	table.sort(copied)
	--print('--- after sorted')
	for k,v in pairs(copied) do
	--	mvcc.dump_key(v)
	end
	assert(util.table_equals(keys, copied, true), "sorting order is wrong")
end)

test("TestMVCCEmptyKeyValue", function (db, stats)
	local ok, r
	ok, r = pcall(db.put, db, stats, "", "", makets(0, 1), nil)
	assert(not ok and r:is('mvcc') and r.args[1] == 'empty_value')
	db:put(stats, "", value1, makets(0, 1))
	local encvk = mvcc.make_key("", 0, makets(0, 1))
	assert(stats.bytes_key == (1 + #encvk) and stats.bytes_val == (#value1 + ffi.sizeof('luact_mvcc_metadata_t')))
	local v = db:get("", makets(0, 1))
	assert(v == value1)
	db:rawscan("", 0, test_key1, #test_key1, nil, function (self, k, kl, v, vl)
	end)
	db:rawscan(test_key1, #test_key1, "", 0, nil, function (self, k, kl, v, vl)
		assert(false, "nothing should be scanned")
	end)
	db:resolve_txn(stats, "", 0, nil, nil, nil, makets(0, 1), txn1)
end, create_db, destroy_db)

test("TestMVCCGetNotExist", function (db, stats)
	local ok, r = pcall(db.get, db, test_key1, makets(0, 1))
	assert(ok and (not r), "get for non-existent key should returns nil")
end, create_db, destroy_db)

test("TestMVCCPutWithTxn", function (db, stats)
	db:put(stats, test_key1, value1, makets(1, 0), txn1)
	for _, ts in ipairs({makets(1, 0), makets(0, 1), makets(2, 0)}) do
		local v = db:get(test_key1, ts, txn1)
		assert(v == value1, 
			"put value in transaction should be seen for read which has greater-equals timestamp and same transaction")			
	end
end, create_db, destroy_db)

test("TestMVCCPutWithoutTxn", function (db, stats)
	db:put(stats, test_key1, value1, makets(1, 0))
	for _, ts in ipairs({makets(1, 0), makets(0, 1), makets(2, 0)}) do
		local v = db:get(test_key1, ts)
		assert(v == value1, 
			"put value in transaction should be seen for read which has greater-equals timestamp and same transaction")			
	end
end, create_db, destroy_db)

test("TestMVCCUpdateExistingKey", function (db, stats)
	db:put(stats, test_key1, value1, makets(1, 0))	
	local v = db:get(test_key1, makets(0, 1))
	assert(v == value1, "get with latest ts should return last put value")
	
	db:put(stats, test_key1, value2, makets(0, 2))
	-- Read the latest version.
	local v = db:get(test_key1, makets(0, 3))
	assert(v == value2, "after multiple version created, get with latest ts should return last put value")
	-- Read the old version.
	local v = db:get(test_key1, makets(0, 1))
	assert(v == value1, "get with specified ts should return last put value before ts")
end, create_db, destroy_db)

test("TestMVCCUpdateExistingKeyOldVersion", function (db, stats)
	db:put(stats, test_key1, value1, makets(1, 1))
	-- Earlier walltime.
	local ok, r = pcall(db.put, db, stats, test_key1, value2, makets(1, 0))
	assert((not ok) and r:is('mvcc') and r.args[1] == 'write_too_old')
	-- Earlier logical time.
	local ok, r = pcall(db.put, db, stats, test_key1, value2, makets(0, 1))
	assert((not ok) and r:is('mvcc') and r.args[1] == 'write_too_old')
end, create_db, destroy_db)


test("TestMVCCUpdateExistingKeyInTxn", function (db, stats)
	db:put(stats, test_key1, value1, makets(1, 0), txn1)
	db:put(stats, test_key1, value1, makets(0, 1), txn1)
end, create_db, destroy_db)

test("TestMVCCUpdateExistingKeyDiffTxn", function (db, stats)
	db:put(stats, test_key1, value1, makets(1, 0), txn1)
	local ok, r = pcall(db.put, db, stats, test_key1, value2, makets(0, 1), txn2)
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists')
end, create_db, destroy_db)

test("TestMVCCGetNoMoreOldVersion", function (db, stats)
	-- Need to handle the case here where the scan takes us to the
	-- next key, which may not match the key we're looking for. In
	-- other words, if we're looking for a<T=2>, and we have the
	-- following keys:
	--
	-- a: MVCCMetadata(a)
	-- a<T=3>
	-- b: MVCCMetadata(b)
	-- b<T=1>
	--
	-- If we search for a<T=2>, the scan should not return "b".
	-- and if we search for a<T=4>, scan should return "a"

	db:put(stats, test_key1, value1, makets(0, 3))
	db:put(stats, test_key2, value2, makets(0, 1))
	
	local v = db:get(test_key1, makets(0, 2))
	assert(not v)
	local v2 = db:get(test_key1, makets(0, 4))
	assert(v2 == value1)
end, create_db, destroy_db)

-- TestMVCCGetUncertainty verifies that the appropriate error results when
-- a transaction reads a key at a timestamp that has versions newer than that
-- timestamp, but older than the transaction's MaxTimestamp.
test("TestMVCCGetUncertainty", function (db, st)
	local ok, r
	local ubk, ubkl
	local txn = new_txn({
		coord = actor.root_of(nil, 100),
		timestamp = makets(0, 5),
		max_ts = makets(0, 10),
	})
	-- Put a value from the past.
	db:put(st, test_key1, value1, makets(0, 1))
	-- Put a value that is ahead of MaxTimestamp, it should not interfere.
	db:put(st, test_key1, value2, makets(0, 12))
	-- Read with transaction, should get a value back.
	local v = db:get(test_key1, makets(0, 7), txn)
	assert(v == value1)
	-- Now using testKey2.
	-- Put a value that conflicts with MaxTimestamp.
	db:put(st, test_key2, value2, makets(0, 9))
	-- Read with transaction, should get error back.
	ubk, ubkl = mvcc.upper_bound_of_prefix(test_key2)

	ok, r = pcall(db.get, db, test_key2, makets(0, 7), txn)
	assert((not ok) and r:is('mvcc') and 
		r.args[1] == 'txn_ts_uncertainty' and 
		r.args[3] == makets(0, 9) and r.args[4] == txn:max_timestamp())

	ok, r = pcall(db.scan, db, test_key2, #test_key2, ubk, ubkl, 10, makets(0, 7), txn)
	assert((not ok) and r:is('mvcc') and 
		r.args[1] == 'txn_ts_uncertainty' and 
		r.args[3] == makets(0, 9) and r.args[4] == txn:max_timestamp())

	-- Adjust MaxTimestamp and retry.
	txn.max_ts = makets(0, 7)
	assert(not db:get(test_key2, makets(0, 7), txn))
	r = db:scan(test_key2, #test_key2, ubk, ubkl, 10, makets(0, 7), txn)
	assert(#r == 0)
	-- restore max_timestamp
	txn.max_ts = makets(0, 10)

	-- Now using testKey3.
	-- Put a value that conflicts with MaxTimestamp and another write further
	-- ahead and not conflicting any longer. The first write should still ruin
	-- it.
	ubk, ubkl = mvcc.upper_bound_of_prefix(test_key3)
	db:put(st, test_key3, value2, makets(0, 9))
	db:put(st, test_key3, value2, makets(0, 99))
	ok, r = pcall(db.scan, db, test_key3, #test_key3, ubk, ubkl, 10, makets(0, 7), txn)
	assert((not ok) and r:is('mvcc') and 
		r.args[1] == 'txn_ts_uncertainty' and 
		r.args[3] == makets(0, 9) and r.args[4] == makets(0, 7))

	ok, r = pcall(db.get, db, test_key3, makets(0, 7), txn)
	assert((not ok) and r:is('mvcc') and 
		r.args[1] == 'txn_ts_uncertainty' and 
		r.args[3] == makets(0, 9) and r.args[4] == makets(0, 7))

end, create_db, destroy_db)

test("TestMVCCGetAndDelete", function (db, st)
	local ek1 = mvcc.make_key(test_key1, #test_key1) 
	local ek2 = mvcc.make_key(test_key1, #test_key1, makets(0, 1))
	db:put(st, test_key1, value1, makets(0, 1))
	assert(st.bytes_key == (#ek1 + #ek2) and st.bytes_val == (#value1 + ffi.sizeof('luact_mvcc_metadata_t')))
	local v = db:get(test_key1, makets(0, 2))
	assert(v == value1)
	-- delete value for test_key1
	db:delete(st, test_key1, makets(0, 3))
	-- new versioned key which value is "" (empty string) is created, so stats will be below
	assert(st.bytes_key == (#ek1 + #ek2 + #ek2) and st.bytes_val == (#value1 + ffi.sizeof('luact_mvcc_metadata_t')))

	-- Read the latest version which should be deleted.
	assert(not db:get(test_key1, makets(0, 4)))	
	-- Read the old version which should still exist.
	local v = db:get(test_key1, makets(0, 2))
	assert(v == value1)
	local v = db:get(test_key1, makets(lamport.MAX_HLC_LOGICAL_CLOCK, 2))
	assert(v == value1)
end, create_db, destroy_db)

test("TestMVCCDeleteMissingKey", function (db, st)
	db:delete(st, test_key1, makets(1, 0))
	-- Verify nothing is written to the engine.
	assert(not db.db:get(test_key1))
end, create_db, destroy_db)

test("TestMVCCGetAndDeleteInTxn", function (db, st)
	db:put(st, test_key1, value1, makets(0, 1), txn1)
	local v = db:get(test_key1, makets(0, 2), txn1)
	assert(v == value1)
	-- delete value
	db:delete(st, test_key1, makets(0, 3), txn1)
	-- Read the latest version which should be deleted.
	assert(not db:get(test_key1, makets(0, 4), txn1))
	-- Read the old version which shouldn't exist, as within a
	-- transaction, we delete previous values.
	assert(not db:get(test_key1, makets(0, 2)))
end, create_db, destroy_db)

test("TestMVCCGetWriteIntentError", function (db, st)
	local ok, r 
	db:put(st, test_key1, value1, makets(0, 1), txn1)
	ok, r = pcall(db.get, db, test_key1, makets(0, 1))
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists', 
		"cannot read the value of a write intent without TxnID")
	ok, r = pcall(db.get, db, test_key1, makets(0, 1), txn2)
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists', 
		"cannot read the value of a write intent from a different TxnID")
	local v = db:get(test_key1, makets(0, 2), txn1)
	assert(v == value1, "in same txn, should get correct value")
end, create_db, destroy_db)

test("TestMVCCScan", function (db, st)
	local fixtures, results, p, len = {
		{test_key1, value1, makets(0, 1)},
		{test_key1, value4, makets(0, 2)},
		{test_key2, value2, makets(0, 1)},
		{test_key2, value3, makets(0, 3)},
		{test_key3, value3, makets(0, 1)},
		{test_key3, value2, makets(0, 4)},
		{test_key4, value4, makets(0, 1)},
		{test_key4, value1, makets(0, 5)},
	}
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end

	results = db:scan(test_key2, #test_key2, test_key4, #test_key4, 0, makets(0, 1))
	assert(#results == 2 and 
		ffi.string(unpack(results[1])) == test_key2 and
		ffi.string(unpack(results[2])) == test_key3 and
		ffi.string(unpack(results[1], 3)) == value2 and
		ffi.string(unpack(results[2], 3)) == value3)

	results = db:scan(test_key2, #test_key2, test_key4, #test_key4, 0, makets(0, 4))
	assert(#results == 2 and 
		ffi.string(unpack(results[1])) == test_key2 and
		ffi.string(unpack(results[2])) == test_key3 and
		ffi.string(unpack(results[1], 3)) == value3 and
		ffi.string(unpack(results[2], 3)) == value2)

	p, len = key.MAX:as_slice()
	results = db:scan(test_key4, #test_key4, p, len, 0, makets(0, 1))
	assert(#results == 1 and 
		ffi.string(unpack(results[1])) == test_key4 and
		ffi.string(unpack(results[1], 3)) == value4, "the value should not be empty")

	-- why original cockroach test do this?
	db:get(test_key1, makets(0, 1), maketxn(txn2, makets(0, 1)))

	p, len = key.MIN:as_slice()
	results = db:scan(p, len, test_key2, #test_key2, 0, makets(0, 1))
	assert(#results == 1 and 
		ffi.string(unpack(results[1])) == test_key1 and
		ffi.string(unpack(results[1], 3)) == value1, "the value should not be empty")

end, create_db, destroy_db)

test("TestMVCCScanMaxNum", function (db, st)
	local fixtures, results = {
		{test_key1, value1, makets(0, 1)},
		{test_key2, value2, makets(0, 1)},
		{test_key3, value3, makets(0, 1)},
		{test_key4, value4, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end

	results = db:scan(test_key2, #test_key2, test_key4, #test_key4, 1, makets(0, 1))
	assert(#results == 1 and 
		ffi.string(unpack(results[1])) == test_key2 and
		ffi.string(unpack(results[1], 3)) == value2, "the value should not be empty")

end, create_db, destroy_db)

test("TestMVCCScanWithKeyPrefix", function (db, st)
	-- Let's say you have:
	-- a
	-- a<T=2>
	-- a<T=1>
	-- aa
	-- aa<T=3>
	-- aa<T=2>
	-- b
	-- b<T=1>
	-- In this case, if we scan from "a"-"b", we wish to skip
	-- a<T=2> and a<T=1> and find "aa'.
	local fixtures, results = {
		{"/a", value1, makets(0, 1)},
		{"/a", value2, makets(0, 2)},
		{"/aa", value2, makets(0, 2)},
		{"/aa", value3, makets(0, 3)},
		{"/b", value3, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end
	results = db:scan("/a", 2, "/b", 2, 0, makets(0, 2))

	assert(#results == 2 and 
		ffi.string(unpack(results[1])) == "/a" and
		ffi.string(unpack(results[2])) == "/aa" and
		ffi.string(unpack(results[1], 3)) == value2 and
		ffi.string(unpack(results[2], 3)) == value2)

end, create_db, destroy_db)

test("TestMVCCScanInTxn", function (db, st)
	local fixtures, results = {
		{test_key1, value1, makets(0, 1)},
		{test_key2, value2, makets(0, 1)},
		{test_key3, value3, makets(0, 1), txn1},
		{test_key4, value4, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end

	results = db:scan(test_key2, #test_key2, test_key4, #test_key4, 0, makets(0, 1), txn1)
	assert(#results == 2 and 
		ffi.string(unpack(results[1])) == test_key2 and
		ffi.string(unpack(results[2])) == test_key3 and
		ffi.string(unpack(results[1], 3)) == value2 and
		ffi.string(unpack(results[2], 3)) == value3, 
		"the value should not be empty")

	local ok, r = pcall(db.scan, db, test_key2, #test_key2, test_key4, #test_key4, 0, makets(0, 1))
	assert(not ok and r:is('mvcc') and r.args[1] == 'txn_exists')
end, create_db, destroy_db)

-- TestMVCCIterateCommitted writes several values, some as intents
-- and verifies that IterateCommitted sees only the committed versions.
test("TestMVCCIterateCommitted", function (db, st)
	local ts1 = makets(0, 1)
	local ts2 = makets(0, 2)
	local ts3 = makets(0, 3)
	local ts4 = makets(0, 4)
	local ts5 = makets(0, 5)
	local ts6 = makets(0, 6)

	local fixtures, results = {
		{test_key1, value1, ts1},
		{test_key1, value2, ts2, txn2},
		{test_key2, value1, ts3},
		{test_key2, value2, ts4},
		{test_key3, value3, ts5, txn2},
		{test_key4, value4, ts6},
	}
	local test_key4_next = test_key4..string.char(0)
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end

	local expects, count = {
		{test_key1, value1, ts1},
		{test_key2, value2, ts4},
		{test_key4, value4, ts6},
	}, 0
	db:scan_committed(test_key1, #test_key1, test_key4_next, #test_key4_next, function (k, kl, v, vl, ts)
		count = count + 1
		-- print(count, ffi.string(k, kl), ffi.string(v, vl), ts)
		assert(util.table_equals(expects[count], {ffi.string(k, kl), ffi.string(v, vl), ts}))
	end)
	assert(count == #expects)
end, create_db, destroy_db)

test("TestMVCCDeleteRange", function (db, st) 
	local fixtures, results = {
		{test_key1, value1, makets(0, 1)},
		{test_key2, value2, makets(0, 1)},
		{test_key3, value3, makets(0, 1)},
		{test_key4, value4, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end

	local n
	n = db:delete_range(st, test_key2, test_key4, 0, makets(0, 2))
	assert(n == 2)

	local mink, minkl = key.MIN:as_slice()
	local maxk, maxkl = key.MAX:as_slice()
	results = db:scan(mink, minkl, maxk, maxkl, 0, makets(0, 2))
	assert(#results == 2 and 
		ffi.string(unpack(results[1])) == test_key1 and
		ffi.string(unpack(results[2])) == test_key4 and
		ffi.string(unpack(results[1], 3)) == value1 and
		ffi.string(unpack(results[2], 3)) == value4, 
		"unremoved value should appear")

	n = db:rawdelete_range(st, test_key4, #test_key4, maxk, maxkl, 0, makets(0, 2))
	assert(n == 1)

	results = db:scan(mink, minkl, maxk, maxkl, 0, makets(0, 2))
	assert(#results == 1 and 
		ffi.string(unpack(results[1])) == test_key1 and
		ffi.string(unpack(results[1], 3)) == value1 and
		"unremoved value should appear")

	n = db:rawdelete_range(st, mink, minkl, test_key2, #test_key2, 0, makets(0, 2))
	results = db:scan(mink, minkl, maxk, maxkl, 0, makets(0, 2))
	assert(#results == 0, "value should not appear")
end, create_db, destroy_db)

test("TestMVCCDeleteRangeFailed", function (db, st)
	local fixtures, results = {
		{test_key1, value1, makets(0, 1)},
		{test_key2, value2, makets(0, 1), txn1},
		{test_key3, value3, makets(0, 1), txn1},
		{test_key4, value4, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end

	local ok, r
	ok, r = pcall(db.delete_range, db, st, test_key2, test_key4, 0, makets(0, 1))
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists')
	
	db:delete_range(st, test_key2, test_key4, 0, makets(0, 1), txn1)
end, create_db, destroy_db)

test("TestMVCCDeleteRangeConcurrentTxn", function (db, st)
	local fixtures, results = {
		{test_key1, value1, makets(0, 1)},
		{test_key2, value2, makets(0, 1), txn1},
		{test_key3, value3, makets(0, 2), txn2}, -- it works for makets(0, 1). why cockroach test uses 0,2 ?
		{test_key4, value4, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end

	local ok, r
	ok, r = pcall(db.delete_range, db, st, test_key2, test_key4, 0, makets(0, 1), txn1)
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists')
end, create_db, destroy_db)

test("TestMVCCConditionalPut", function (db, st)
	local ok, r, v
	ok, r = db:cas(st, test_key1, "", value1, makets(1, 0))
	assert(ok and (not r))
	v = db:get(test_key1, makets(1, 0))
	assert(v == value1)
	-- Conditional put expecting wrong value2, will fail.
	ok, r = db:cas(st, test_key1, value2, value3, makets(0, 1))
	assert((not ok) and r == value1)
	-- move to empty value will success
	ok, r = db:cas(st, test_key1, value1, "", makets(0, 1))
	assert(ok and r == value1)
end, create_db, destroy_db)

test("TestMVCCResolveTxn", function (db, st)
	local v
	db:put(st, test_key1, value1, makets(0, 1), txn1)
	v = db:get(test_key1, makets(0, 1), txn1)
	assert(v == value1)
	local ok, r = pcall(db.get, db, test_key1, makets(0, 1))
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists')
	-- Resolve will write with txn1's timestamp which is 1, 0
	db:resolve_txn(st, test_key1, #test_key1, makets(0, 1), txn1_commit)
	-- now non-transactional get can see the value put at 1, 0
	v = db:get(test_key1, makets(0, 1))
	assert(v == value1)
end, create_db, destroy_db)

test("TestMVCCAbortTxn", function (db, st)
	db:put(st, test_key1, value1, makets(0, 1), txn1)
	db:resolve_txn(st, test_key1, #test_key1, makets(0, 1), txn1_abort)

	assert(not db:get(test_key1, makets(1, 0)), "value should be empty")
	local meta_key = mvcc.make_key(test_key1, #test_key1)
	local v, vl = db.db:rawget(meta_key, #meta_key)
	assert(not db.db:get(meta_key), "expected no more metadata")
end, create_db, destroy_db)

test("TestMVCCAbortTxnWithPreviousVersion", function (db, st)
	local fixtures, results = {
		{test_key1, value1, makets(1, 0)},
		{test_key1, value2, makets(0, 1)},
		{test_key1, value3, makets(0, 2), txn1},
	}
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end

	db:resolve_txn(st, test_key1, #test_key1, makets(0, 2), txn1_abort)

	local meta_key = mvcc.make_key(test_key1, #test_key1)
	local meta, ml = db.db:rawget(meta_key, #meta_key)
	assert(meta ~= ffi.NULL and ml == ffi.sizeof('luact_mvcc_metadata_t'))

	local v, ts = db:get(test_key1, makets(0, 3))
	assert(v == value2 and ts == makets(0, 1))
end, create_db, destroy_db)

test("TestMVCCWriteWithDiffTimestampsAndEpochs", function (db, st)
	local fixtures, results = {
		{test_key1, value1, makets(1, 0), txn1}, -- ok. initial write
		{test_key1, value2, makets(0, 1), txn1_1}, -- ok, greater ts and n_retry
		{test_key1, value1, makets(1, 0), txn1_1}, -- should ignore. smaller ts
		{test_key1, value1, makets(0, 1), txn1}, -- should ignore. smaller n_retry
		{test_key1, value3, makets(0, 1), txn1_1}, -- ok. greater n_retry, same ts
	}
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end

	local ok, r, v, ts
	-- Resolve the txn.
	db:resolve_txn(st, test_key1, #test_key1, makets(0, 1), maketxn(txn1_1_commit, makets(0, 2)))
	-- Now try writing an earlier intent--should get write too old error.
	ok, r = pcall(db.put, db, st, test_key1, value2, makets(1, 0), txn2)
	assert((not ok) and r:is('mvcc') and r.args[1] == 'write_too_old')
	-- Attempt to read older timestamp; should fail.
	v = db:get(test_key1, makets(0, 0))
	assert(not v)
	-- Read at correct timestamp.
	v, ts = db:get(test_key1, makets(0, 2))
	assert(v == value3 and ts == makets(0, 2))
end, create_db, destroy_db)

-- TestMVCCReadWithDiffEpochs writes a value first using epoch 1, then
-- reads using epoch 2 to verify that values written during different
-- transaction epochs are not visible.
test("TestMVCCReadWithDiffEpochs", function (db, st)
	-- Write initial value wihtout a txn.
	db:put(st, test_key1, value1, makets(1, 0))
	db:put(st, test_key1, value2, makets(0, 1), txn1)
	-- Try reading using different txns & epochs.
	local fixtures = {
		-- No transaction; should see error.
		{nil, nil, true},
		-- Txn1, no retry; should see new value2.
		{txn1, value2, false},
		-- Txn1, retry = 1; should see original value1.
		{txn1_1, value1, false},
		-- Txn2; should see error.
		{txn2, nil, true},
	}
	for _, test in ipairs(fixtures) do
		local ok, v = pcall(db.get, db, test_key1, makets(0, 2), test[1])
		if test[3] then
			assert((not ok) and v:is('mvcc') and v.args[1] == 'txn_exists')
		else
			assert(v == test[2])
		end
	end
end, create_db, destroy_db)

-- TestMVCCReadWithPushedTimestamp verifies that a read for a value
-- written by the transaction, but then subsequently pushed, can still
-- be read by the txn at the later timestamp, even if an earlier
-- timestamp is specified. This happens when a txn's intents are
-- resolved by other actors; the intents shouldn't become invisible
-- to pushed txn.
test("TestMVCCReadWithPushedTimestamp", function(db, st)
	-- Start with epoch 1.
	db:put(st, test_key1, value1, makets(1, 0), txn1)
	db:resolve_txn(st, test_key1, #test_key1, makets(1, 0), maketxn(txn1, makets(0, 1)))
	-- Attempt to read using naive txn's previous timestamp.
	local v = db:get(test_key1, makets(1, 0), txn1)
	assert(v == value1)
end, create_db, destroy_db)

test("TestMVCCResolveWithDiffEpochs", function (db, st)
	db:put(st, test_key1, value1, makets(0, 1), txn1)
	db:put(st, test_key2, value2, makets(0, 1), txn1_1)
	local test_key2_next = test_key2..string.char(0)
	local num = db:end_txn(st, test_key1, #test_key1, test_key2_next, #test_key2_next, 2, makets(0, 1), txn1_1_commit)
	assert(num == 2)

	-- Verify key1 is empty, as resolution with epoch 2 would have
	-- aborted the epoch 1 intent.
	assert(not db:get(test_key1, makets(0, 1)))

	-- Key2 should be committed.
	local v = db:get(test_key2, makets(0, 1))
	assert(v == value2)
end, create_db, destroy_db)

test("TestMVCCResolveWithUpdatedTimestamp", function (db, st)
	local v
	db:put(st, test_key1, value1, makets(0, 1), txn1)
	v = db:get(test_key1, makets(1, 1), txn1)
	assert(v == value1)

	-- Resolve with a higher commit timestamp -- this should rewrite the
	-- intent when making it permanent.
	db:resolve_txn(st, test_key1, #test_key1, makets(1, 1), maketxn(txn1_commit, makets(1, 1)))

	v = db:get(test_key1, makets(0, 1))
	assert(not v)

	v = db:get(test_key1, makets(1, 1))	
	assert(v == value1)
end, create_db, destroy_db)

test("TestMVCCResolveWithPushedTimestamp", function (db, st)
	local ok, v
	db:put(st, test_key1, value1, makets(0, 1), txn1)
	v = db:get(test_key1, makets(0, 1), txn1)
	assert(v == value1)

	-- Resolve with a higher commit timestamp, but with still-pending transaction.
	-- This represents a straightforward push (i.e. from a read/write conflict).
	db:resolve_txn(st, test_key1, #test_key1, makets(1, 1), maketxn(txn1, makets(1, 1))[0])

	ok, v = pcall(db.get, db, test_key1, makets(1, 1))
	-- because test_key1 is not committed, just pushed, so read latest version without txn will be failure
	assert((not ok) and v:is('mvcc') and v.args[1] == 'txn_exists') 

	-- Can still fetch the value using txn1.
	v = db:get(test_key1, makets(1, 1), txn1)	
	assert(v == value1)
end, create_db, destroy_db)

test("TestMVCCResolveTxnNoOps", function (db, st)
	-- Resolve a non existent key; noop.
	local ok, r = pcall(db.resolve_txn, db, st, test_key1, #test_key1, makets(0, 1), txn1_commit)
	assert(ok)

	-- Add key and resolve despite there being no intent.
	db:put(st, test_key1, value1, makets(0, 1))
	db:resolve_txn(st, test_key1, #test_key1, makets(0, 1), txn2_commit)

	-- Write intent and resolve with different txn.
	db:put(st, test_key1, value2, makets(0, 1), txn1)
	db:resolve_txn(st, test_key1, #test_key1, makets(0, 1), txn2_commit)
end, create_db, destroy_db)

test("TestMVCCResolveTxnRange", function (db, st)
	local fixtures, results = {
		{test_key1, value1, makets(0, 1), txn1},
		{test_key2, value2, makets(0, 1), nil},
		{test_key3, value3, makets(0, 1), txn2},
		{test_key4, value4, makets(0, 1), txn1},
	}
	local test_key4_next = test_key4..string.char(0)
	for _, data in ipairs(fixtures) do
		db:put(st, unpack(data))
	end

	local num = db:end_txn(st, test_key1, #test_key1, test_key4_next, #test_key4_next, 0, makets(0, 1), txn1_commit)
	assert(num == 4)

	local v
	v = db:get(test_key1, makets(0, 1))
	assert(v == value1)

	v = db:get(test_key2, makets(0, 1))
	assert(v == value2)

	v = db:get(test_key3, makets(0, 1), txn2)
	assert(v == value3)

	v = db:get(test_key4, makets(0, 1))
	assert(v == value4)
end, create_db, destroy_db)

test("TestFindSplitKey", function (db, st)
	-- Generate a series of KeyValues, each containing targetLength
	-- bytes, writing key #i to (encoded) key #i through the MVCC
	-- facility. Assuming that this translates roughly into same-length
	-- values after MVCC encoding, the split key should hence be chosen
	-- as the middle key of the interval.
	for i=1,999 do
		local k = ('%03d'):format(i)
		local v = ("x"):rep(1 + math.abs(500 - i))
		-- Write the key and value through MVCC
		db:put(st, k, v, makets(0, 1))
	end
	local mink, minkl = key.MIN:as_slice()
	local maxk, maxkl = key.MAX:as_slice()
	local k, kl = db:find_split_key(st, mink, minkl, maxk, maxkl)
	assert(ffi.string(k, kl) == tostring(500))
end, create_db, destroy_db)

-- TestFindValidSplitKeys verifies split keys are located such that
-- they avoid splits through invalid key ranges.
test("TestFindValidSplitKeys", function (db, st)
	for i=1,999 do
		local k = ('%03d'):format(i)
		local v = ("x"):rep(1 + 7)--math.abs(500 - i))
		-- Write the key and value through MVCC
		db:put(st, k, v, makets(0, 1))
	end
	local mink, minkl = key.MIN:as_slice()
	local maxk, maxkl = key.MAX:as_slice()
	local k, kl 
	k, kl = db:find_split_key(st, mink, minkl, maxk, maxkl, function (k, kl)
		local s = ffi.string(k, kl)
		return s <= "402" or s >= "599"
	end)
	-- actually, last of 401 is best split point (and it is enable), so 402 will be first key of splitted range
	assert(k and ffi.string(k, kl) == tostring(402))
	k, kl = db:find_split_key(st, mink, minkl, maxk, maxkl, function (k, kl)
		local s = ffi.string(k, kl)
		return false
	end)
	assert(not k)
end, create_db, destroy_db)

-- TestFindBalancedSplitKeys verifies split keys are located such that
-- the left and right halves are equally balanced.
test("TestFindBalancedSplitKeys", function (db, st)
	local fixtures = {
		-- Bigger keys on right side.
		{
			keysizes = {10, 100, 10, 10, 500},
			valsizes = {1, 1, 1, 1, 1},
			exp_split = 4,
		},
		-- Bigger keys on left side.
		{
			keysizes = {1000, 100, 500, 10, 10},
			valsizes = {1, 1, 1, 1, 1},
			exp_split = 1,
		},
		-- Bigger values on right side.
		{
			keysizes = {1, 1, 1, 1, 1},
			valsizes = {10, 100, 10, 10, 500},
			exp_split = 4,
		},
		-- Bigger values on left side.
		{
			keysizes = {1, 1, 1, 1, 1},
			valsizes = {1000, 100, 500, 10, 10},
			exp_split = 1,
		},
		-- Bigger key/values on right side.
		{
			keysizes = {10, 100, 10, 10, 250},
			valsizes = {10, 100, 10, 10, 250},
			exp_split = 4,
		},
		-- Bigger key/values on left side.
		{
			keysizes = {500, 50, 250, 10, 10},
			valsizes = {500, 50, 250, 10, 10},
			exp_split = 1,
		},
	}

	local mink, minkl = key.MIN:as_slice()
	local maxk, maxkl = key.MAX:as_slice()

	for idx, data in ipairs(fixtures) do
		st:init()
		local cf = db:column_family(tostring(idx))
		local exp_key 
		for i=1,#data.keysizes do
			local sz = data.keysizes[i]
			local k = string.char(i)..(("x"):rep(sz))
			local v = ("x"):rep(data.valsizes[i])
			if data.exp_split == (i - 1) then
				exp_key = k
			end
			cf:put(st, k, v, makets(0, 1))
		end
		local spk, spkl = cf:find_split_key(st, mink, minkl, maxk, maxkl)
		--print(idx, ('%q'):format(exp_key), ('%q'):format(ffi.string(spk, spkl)))
		assert(spk and ffi.string(spk, spkl) == exp_key)
		cf:fin()
	end
end, create_db, destroy_db)


function make_stats(k, v, n_k, n_v, uncommitted_bytes)
	local p = ffi.new('luact_mvcc_stats_t')
	p.bytes_key = k
	p.bytes_val = v
	p.n_key = n_k
	p.n_val = n_v
	p.uncommitted_bytes = uncommitted_bytes
	return p
end
-- TestMVCCStatsBasic writes a value, then deletes it as an intent via
-- a transaction, then resolves the intent, manually verifying the
-- mvcc stats at each step.
test("TestMVCCStatsBasic", function (db, st)
	-- Put a value.
	local ts = makets(0, 1*1E9)
	local key = "a"
	local value = "value"
	db:put(st, key, value, ts)
	local m_key_size = encode_size(key)
	local m_val_size = ffi.sizeof('luact_mvcc_metadata_t')
	local v_key_size = encode_size(key, nil, ts)
	local v_val_size = #value
	local exp_stats = make_stats(
		m_key_size + v_key_size, 
		m_val_size + v_val_size, 
		0, 
		1, 1, 0)
	assert(verify_stats(exp_stats, st), "check after put fails")


	-- Delete the value using a transaction.
	local ts2 = makets(0, 2*1E9)
	local txn = maketxn(txn1, makets(0, 1*1E9))[0]
	db:delete(st, key, ts2, txn)
	local m2_val_size = ffi.sizeof('luact_mvcc_metadata_t')
	local v2_key_size = encode_size(key, nil, ts2)
	local v2_val_size = 0 -- deleted value size is 0
	local exp_stats2 = make_stats(
		m_key_size + v_key_size + v2_key_size, 
		m2_val_size + v_val_size + v2_val_size,
		v2_key_size + v2_val_size,
		1, 2, 1)
	assert(verify_stats(exp_stats2, st), "check after uncommitted delete fails")


	-- Resolve the deletion by aborting it.
	txn.status = txncoord.STATUS_ABORTED
	db:resolve_txn(st, key, #key, ts2, txn)
	-- Stats should equal same as before the deletion after aborting the intent.
	assert(verify_stats(exp_stats, st), "check after txn abort fails")


	-- Re-delete, but this time, we're going to commit it.
	txn.status = txncoord.STATUS_PENDING
	local ts3 = makets(0, 3*1E9)
	db:delete(st, key, ts3, txn)
	assert(verify_stats(exp_stats2, st), "check after re-delete txn fails")


	-- Put another intent, which should age deleted intent.
	local ts4 = makets(0, 4*1E9)
	txn.timestamp = ts4
	local key2 = "b"
	local value2 = "value2"
	db:put(st, key2, value2, ts4, txn)
	local m_key2_size = encode_size(key2)
	local m_val2_size = ffi.sizeof('luact_mvcc_metadata_t')
	local v_key2_size = encode_size(key2, nil, ts4)
	local v_val2_size = #value2
	local exp_stats3 = make_stats(
		m_key_size + v_key_size + v2_key_size + m_key2_size + v_key2_size,
		m2_val_size + v_val_size + v2_val_size + m_val2_size + v_val2_size,
		v2_key_size + v2_val_size + v_key2_size + v_val2_size,
		2, 3, 2)
	assert(verify_stats(exp_stats3, st), "check after put another value fails")

	-- Now commit both values.
	txn.status = txncoord.STATUS_COMMITTED
	db:resolve_txn(st, key, #key, ts4, txn)
	db:resolve_txn(st, key2, #key2, ts4, txn)
	local m3_val_size = ffi.sizeof('luact_mvcc_metadata_t')
	local m2_val2_size = ffi.sizeof('luact_mvcc_metadata_t')
	local exp_stats4 = make_stats(
		m_key_size + v_key_size + v2_key_size + m_key2_size + v_key2_size,
		m3_val_size + v_val_size + v2_val_size + m2_val2_size + v_val2_size,
		0, 
		2, 3, 0)
	assert(verify_stats(exp_stats4, st), "check after commit fails")

	-- Write over existing value to create GC'able bytes.
	local ts5 = makets(0, 10*1E9) -- skip ahead 6s
	db:put(st, key2, value2, ts5)
	local exp_stats5 = make_stats(
		m_key_size + v_key_size + v2_key_size + m_key2_size + v_key2_size + v_key2_size,
		m3_val_size + v_val_size + v2_val_size + m2_val2_size + v_val2_size + v_val2_size,
		0, 
		2, 4, 0)
	-- expMS5.GCBytesAge += (vKey2Size + vVal2Size) * 6 // since we skipped ahead 6s
	assert(verify_stats(exp_stats5, st), "check after overwrite fails")
end, create_db, destroy_db)


-- TestMVCCStatsWithRandomRuns creates a random sequence of puts,
-- deletes and delete ranges and at each step verifies that the mvcc
-- stats match a manual computation of range stats via a scan of the
-- underlying engine.
test("TestMVCCStatsWithRandomRuns", function (db, st)
	-- Now, generate a random sequence of puts, deletes and resolves.
	-- Each put and delete may or may not involve a txn. Resolves may
	-- either commit or abort.
	local keys = {}
	local mink, minkl = key.MIN:as_slice()
	local maxk, maxkl = key.MAX:as_slice()
	for i=1,1000 do
		local key = tostring(i)..('x'):rep(math.random(1, 63))
		table.insert(keys, key)
		local txn
		local ts = makets(0, 100 * i)
		local maxts = makets(0, 100 * i + 10)
		if math.random(1, 2) == 1 then
			-- print(i, 'use txn')
			txn = new_txn({
				coord = actor.root_of(nil, i),
				timestamp = ts,
				max_ts = maxts,
			})
		end
		local delete = (math.random(1, 4) == 1)
		if delete then
			local idx = math.random(1, i)
			-- print(i, 'delete key', ('%q'):format(keys[idx]))
			local ok, r = pcall(db.delete, db, st, keys[idx], ts, txn)
			if not ok then
				-- if transaction exists, abort it
				assert(r:is('mvcc') and r.args[1] == 'txn_exists', tostring(r))
				-- print(i, 'delete key', 'remove prev txn')
				local exist_txn = r.args[3]
				exist_txn.status = txncoord.STATUS_ABORTED
				db:resolve_txn(st, keys[idx], #keys[idx], ts, exist_txn)
				db:delete(st, keys[idx], ts, txn)
			end
		else
			-- print(i, 'put key', key)
			db:put(st, key, ('x'):rep(math.random(1, 255)), ts, txn)
		end

		if (not delete) and txn and (math.random(1, 2) == 1) then -- resolve txn with 50% prob
			-- print(i, 'resolve_txn', ('%q'):format(key))
			if math.random(1, 10) == 1 then
				txn.status = txncoord.STATUS_ABORTED
			else
				txn.status = txncoord.STATUS_COMMITTED
			end
			db:resolve_txn(st, key, #key, ts, txn)
		end
		-- Every 10th step, verify the stats via manual engine scan.
		if i%10 == 0 then
			-- print(i, 'check')
			-- Compute the stats manually.
			local exp_stats = db:compute_stats(mink, minkl, maxk, maxkl, ts)
			if not verify_stats(exp_stats, st) then
				print(i, 'check fails', 
					exp_stats.bytes_key, exp_stats.bytes_val, 
					st.bytes_key, st.bytes_val)
				assert(false)
			end
		end
	end
end, create_db, destroy_db)

-- TestMVCCGarbageCollect writes a series of gc'able bytes and then
-- sends an MVCC GC request and verifies cleared values and updated
-- stats.
test("TestMVCCGarbageCollect", function (db, st)
	local bytes = "value"
	local tss = { 
		makets(0, 1E9),
		makets(0, 2E9),
		makets(0, 3E9),
	}
	local val1 = bytes
	local val2 = bytes
	local val3 = bytes

	local mink, minkl = key.MIN:as_slice()
	local maxk, maxkl = key.MAX:as_slice()

	local testData = {
		{"a", {val1, val2}, false},
		{"a-del", {val1, val2}, true},
		{"b", {val1, val2, val3}, false},
		{"b-del", {val1, val2, val3}, true},
	}

	for i=1,3 do
		-- Manually advance aggregate gc'able bytes age based on one extra second of simulation.
		-- ms.GCBytesAge += ms.KeyBytes + ms.ValBytes - ms.LiveBytes

		for _, test in ipairs(testData) do
			for _, val in ipairs({unpack(test[2], i, i+1)}) do
				if i == #test[2] and test[3] then
					db:delete(st, test[1], tss[i])
				else
					db:put(st, test[1], val, tss[i])
				end
			end
		end
	end
	-- local results = db:scan_all(ffi.string(mink, minkl), ffi.string(maxk, maxkl), 0)
	-- for i=1,#results do
	-- 	mvcc.dump_key(results[i][1], results[i][2])
	-- end

	local gc_keys = {
		mvcc.make_key("a", 1, tss[1]),
		mvcc.make_key("a-del", 5, tss[2]),
		mvcc.make_key("b", 1, tss[1]),
		mvcc.make_key("b-del", 5, tss[2]),
	}
	local exp_stats_prev = db:compute_stats(mink, minkl, maxk, maxkl, tss[3])
	assert(verify_stats(exp_stats_prev, st))

	print('call GC ------')
	db:gc(st, gc_keys)
	local results2 = db:scan_all(ffi.string(mink, minkl), ffi.string(maxk, maxkl), 0)

	print('after GC -------')	-- 
	local exp_keys = {
		mvcc.make_key("a", 1),
		mvcc.make_key("a", 1, tss[2]),
		mvcc.make_key("b", 1),
		mvcc.make_key("b", 1, tss[2]),
		mvcc.make_key("b", 1, tss[3]),
		mvcc.make_key("b-del", 5),
		mvcc.make_key("b-del", 5, tss[3]),
	}
	-- for i=1,#results2 do
	-- 	mvcc.dump_key(results2[i][1], #results2[i][1])
	-- end
	assert(#results2 == #exp_keys)
	for i=1,#exp_keys do
		assert(exp_keys[i] == results2[i][1])
	end
	-- Verify aggregated stats match computed stats after GC.
	local exp_stats = db:compute_stats(mink, minkl, maxk, maxkl, tss[3])
	assert(verify_stats(exp_stats, st))
end, create_db, destroy_db)

-- TestMVCCGarbageCollectNonDeleted verifies that the first value for
-- a key cannot be GC'd if it's not deleted.
test("TestMVCCGarbageCollectNonDeleted", function (db, st)
	local ts1 = makets(0, 1E9)
	local ts2 = makets(0, 2E9)
	local key = "a"
	for _, ts in ipairs({ts1, ts2}) do
		db:put(st, key, "value", ts)
	end
	local gc_keys = {
		mvcc.make_key(key, #key, ts2)
	}
	local ok, r = pcall(db.gc, db, st, gc_keys)
	assert((not ok) and r:is('mvcc') and r.args[1] == 'gc_non_deleted_value')
end, create_db, destroy_db)

-- TestMVCCGarbageCollectIntent verifies that an intent cannot be GC'd.
test("TestMVCCGarbageCollectIntent", function (db, st)
	local ts1 = makets(0, 1E9)
	local ts2 = makets(0, 2E9)
	local val1 = "value"
	local key = "a"
	db:put(st, key, val1, ts1)
	local txn = maketxn(txn1, ts2)
	db:delete(st, key, ts2, txn[0])
	local gc_keys = {
		mvcc.make_key(key, #key, ts2)
	}
	local ok, r = pcall(db.gc, db, st, gc_keys)
	assert((not ok) and r:is('mvcc') and r.args[1] == 'gc_uncommitted_value')
end, create_db, destroy_db)

-- ]] --

end)


return true
