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
	return mvcc.open('/tmp/luact/txntest')
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

test("TestMVCCEmptyKeyValue", function (db)
	local ok, r
	ok, r = pcall(db.put, db, "", "", makets(0, 1), nil)
	assert(not ok and r:is('mvcc') and r.args[1] == 'empty_value')
	db:put("", value1, makets(0, 1))
	local v = db:get("", makets(0, 1))
	assert(v == value1)
	db:rawscan("", 0, test_key1, #test_key1, nil, function (self, k, kl, v, vl)
	end)
	db:rawscan(test_key1, #test_key1, "", 0, nil, function (self, k, kl, v, vl)
		assert(false, "nothing should be scanned")
	end)
	db:resolve_txn("", 0, nil, nil, nil, makets(0, 1), txn1)
end, create_db, destroy_db)

test("TestMVCCGetNotExist", function (db)
	local ok, r = pcall(db.get, db, test_key1, makets(0, 1))
	assert(ok and (not r), "get for non-existent key should returns nil")
end, create_db, destroy_db)

test("TestMVCCPutWithTxn", function (db)
	db:put(test_key1, value1, makets(1, 0), txn1)
	for _, ts in ipairs({makets(1, 0), makets(0, 1), makets(2, 0)}) do
		local v = db:get(test_key1, ts, txn1)
		assert(v == value1, 
			"put value in transaction should be seen for read which has greater-equals timestamp and same transaction")			
	end
end, create_db, destroy_db)

test("TestMVCCPutWithoutTxn", function (db)
	db:put(test_key1, value1, makets(1, 0))
	for _, ts in ipairs({makets(1, 0), makets(0, 1), makets(2, 0)}) do
		local v = db:get(test_key1, ts)
		assert(v == value1, 
			"put value in transaction should be seen for read which has greater-equals timestamp and same transaction")			
	end
end, create_db, destroy_db)

test("TestMVCCUpdateExistingKey", function (db)
	db:put(test_key1, value1, makets(1, 0))	
	local v = db:get(test_key1, makets(0, 1))
	assert(v == value1, "get with latest ts should return last put value")
	
	db:put(test_key1, value2, makets(0, 2))
	-- Read the latest version.
	local v = db:get(test_key1, makets(0, 3))
	assert(v == value2, "after multiple version created, get with latest ts should return last put value")
	-- Read the old version.
	local v = db:get(test_key1, makets(0, 1))
	assert(v == value1, "get with specified ts should return last put value before ts")
end, create_db, destroy_db)

test("TestMVCCUpdateExistingKeyOldVersion", function (db)
	db:put(test_key1, value1, makets(1, 1))
	-- Earlier walltime.
	local ok, r = pcall(db.put, db, test_key1, value2, makets(1, 0))
	assert((not ok) and r:is('mvcc') and r.args[1] == 'write_too_old')
	-- Earlier logical time.
	local ok, r = pcall(db.put, db, test_key1, value2, makets(0, 1))
	assert((not ok) and r:is('mvcc') and r.args[1] == 'write_too_old')
end, create_db, destroy_db)


test("TestMVCCUpdateExistingKeyInTxn", function (db)
	db:put(test_key1, value1, makets(1, 0), txn1)
	db:put(test_key1, value1, makets(0, 1), txn1)
end, create_db, destroy_db)

test("TestMVCCUpdateExistingKeyDiffTxn", function (db)
	db:put(test_key1, value1, makets(1, 0), txn1)
	local ok, r = pcall(db.put, db, test_key1, value2, makets(0, 1), txn2)
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists')
end, create_db, destroy_db)

test("TestMVCCGetNoMoreOldVersion", function (db)
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

	db:put(test_key1, value1, makets(0, 3))
	db:put(test_key2, value2, makets(0, 1))
	
	local v = db:get(test_key1, makets(0, 2))
	assert(not v)
	local v2 = db:get(test_key1, makets(0, 4))
	assert(v2 == value1)
end, create_db, destroy_db)

-- TestMVCCGetUncertainty verifies that the appropriate error results when
-- a transaction reads a key at a timestamp that has versions newer than that
-- timestamp, but older than the transaction's MaxTimestamp.
test("TestMVCCGetUncertainty", function (db)
	local ok, r
	local ubk, ubkl
	local txn = new_txn({
		coord = actor.root_of(nil, 100),
		timestamp = makets(0, 5),
		max_ts = makets(0, 10),
	})
	-- Put a value from the past.
	db:put(test_key1, value1, makets(0, 1))
	-- Put a value that is ahead of MaxTimestamp, it should not interfere.
	db:put(test_key1, value2, makets(0, 12))
	-- Read with transaction, should get a value back.
	local v = db:get(test_key1, makets(0, 7), txn)
	assert(v == value1)
	-- Now using testKey2.
	-- Put a value that conflicts with MaxTimestamp.
	db:put(test_key2, value2, makets(0, 9))
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
	db:put(test_key3, value2, makets(0, 9))
	db:put(test_key3, value2, makets(0, 99))
	ok, r = pcall(db.scan, db, test_key3, #test_key3, ubk, ubkl, 10, makets(0, 7), txn)
	assert((not ok) and r:is('mvcc') and 
		r.args[1] == 'txn_ts_uncertainty' and 
		r.args[3] == makets(0, 9) and r.args[4] == makets(0, 7))

	ok, r = pcall(db.get, db, test_key3, makets(0, 7), txn)
	assert((not ok) and r:is('mvcc') and 
		r.args[1] == 'txn_ts_uncertainty' and 
		r.args[3] == makets(0, 9) and r.args[4] == makets(0, 7))

end, create_db, destroy_db)

test("TestMVCCGetAndDelete", function (db)
	db:put(test_key1, value1, makets(0, 1))
	local v = db:get(test_key1, makets(0, 2))
	assert(v == value1)
	-- delete value for test_key1
	db:delete(test_key1, makets(0, 3))

	-- Read the latest version which should be deleted.
	assert(not db:get(test_key1, makets(0, 4)))	
	-- Read the old version which should still exist.
	local v = db:get(test_key1, makets(0, 2))
	assert(v == value1)
	local v = db:get(test_key1, makets(lamport.MAX_HLC_LOGICAL_CLOCK, 2))
	assert(v == value1)
end, create_db, destroy_db)

test("TestMVCCDeleteMissingKey", function (db)
	db:delete(test_key1, makets(1, 0))
	-- Verify nothing is written to the engine.
	assert(not db.db:get(test_key1))
end, create_db, destroy_db)

test("TestMVCCGetAndDeleteInTxn", function (db)
	db:put(test_key1, value1, makets(0, 1), txn1)
	local v = db:get(test_key1, makets(0, 2), txn1)
	assert(v == value1)
	-- delete value
	db:delete(test_key1, makets(0, 3), txn1)
	-- Read the latest version which should be deleted.
	assert(not db:get(test_key1, makets(0, 4), txn1))
	-- Read the old version which shouldn't exist, as within a
	-- transaction, we delete previous values.
	assert(not db:get(test_key1, makets(0, 2)))
end, create_db, destroy_db)

test("TestMVCCGetWriteIntentError", function (db)
	local ok, r 
	db:put(test_key1, value1, makets(0, 1), txn1)
	ok, r = pcall(db.get, db, test_key1, makets(0, 1))
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists', 
		"cannot read the value of a write intent without TxnID")
	ok, r = pcall(db.get, db, test_key1, makets(0, 1), txn2)
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists', 
		"cannot read the value of a write intent from a different TxnID")
	local v = db:get(test_key1, makets(0, 2), txn1)
	assert(v == value1, "in same txn, should get correct value")
end, create_db, destroy_db)

test("TestMVCCScan", function (db)
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
		db:put(unpack(data))
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

test("TestMVCCScanMaxNum", function (db)
	local fixtures, results = {
		{test_key1, value1, makets(0, 1)},
		{test_key2, value2, makets(0, 1)},
		{test_key3, value3, makets(0, 1)},
		{test_key4, value4, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(unpack(data))
	end

	results = db:scan(test_key2, #test_key2, test_key4, #test_key4, 1, makets(0, 1))
	assert(#results == 1 and 
		ffi.string(unpack(results[1])) == test_key2 and
		ffi.string(unpack(results[1], 3)) == value2, "the value should not be empty")

end, create_db, destroy_db)

test("TestMVCCScanWithKeyPrefix", function (db)
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
		db:put(unpack(data))
	end
	results = db:scan("/a", 2, "/b", 2, 0, makets(0, 2))

	assert(#results == 2 and 
		ffi.string(unpack(results[1])) == "/a" and
		ffi.string(unpack(results[2])) == "/aa" and
		ffi.string(unpack(results[1], 3)) == value2 and
		ffi.string(unpack(results[2], 3)) == value2)

end, create_db, destroy_db)

test("TestMVCCScanInTxn", function (db)
	local fixtures, results = {
		{test_key1, value1, makets(0, 1)},
		{test_key2, value2, makets(0, 1)},
		{test_key3, value3, makets(0, 1), txn1},
		{test_key4, value4, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(unpack(data))
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
test("TestMVCCIterateCommitted", function (db)
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
		db:put(unpack(data))
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

test("TestMVCCDeleteRange", function (db) 
	local fixtures, results = {
		{test_key1, value1, makets(0, 1)},
		{test_key2, value2, makets(0, 1)},
		{test_key3, value3, makets(0, 1)},
		{test_key4, value4, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(unpack(data))
	end

	local n
	n = db:delete_range(test_key2, test_key4, 0, makets(0, 2))
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

	n = db:rawdelete_range(test_key4, #test_key4, maxk, maxkl, 0, makets(0, 2))
	assert(n == 1)

	results = db:scan(mink, minkl, maxk, maxkl, 0, makets(0, 2))
	assert(#results == 1 and 
		ffi.string(unpack(results[1])) == test_key1 and
		ffi.string(unpack(results[1], 3)) == value1 and
		"unremoved value should appear")

	n = db:rawdelete_range(mink, minkl, test_key2, #test_key2, 0, makets(0, 2))
	results = db:scan(mink, minkl, maxk, maxkl, 0, makets(0, 2))
	assert(#results == 0, "value should not appear")
end, create_db, destroy_db)

test("TestMVCCDeleteRangeFailed", function (db)
	local fixtures, results = {
		{test_key1, value1, makets(0, 1)},
		{test_key2, value2, makets(0, 1), txn1},
		{test_key3, value3, makets(0, 1), txn1},
		{test_key4, value4, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(unpack(data))
	end

	local ok, r
	ok, r = pcall(db.delete_range, db, test_key2, test_key4, 0, makets(0, 1))
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists')
	
	db:delete_range(test_key2, test_key4, 0, makets(0, 1), txn1)
end, create_db, destroy_db)

test("TestMVCCDeleteRangeConcurrentTxn", function (db)
	local fixtures, results = {
		{test_key1, value1, makets(0, 1)},
		{test_key2, value2, makets(0, 1), txn1},
		{test_key3, value3, makets(0, 2), txn2}, -- it works for makets(0, 1). why cockroach test uses 0,2 ?
		{test_key4, value4, makets(0, 1)},
	}
	for _, data in ipairs(fixtures) do
		db:put(unpack(data))
	end

	local ok, r
	ok, r = pcall(db.delete_range, db, test_key2, test_key4, 0, makets(0, 1), txn1)
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists')
end, create_db, destroy_db)

test("TestMVCCConditionalPut", function (db)
	local ok, r, v
	ok, r = db:cas(test_key1, "", value1, makets(1, 0))
	assert(ok and (not r))
	v = db:get(test_key1, makets(1, 0))
	assert(v == value1)
	-- Conditional put expecting wrong value2, will fail.
	ok, r = db:cas(test_key1, value2, value3, makets(0, 1))
	assert((not ok) and r == value1)
	-- move to empty value will success
	ok, r = db:cas(test_key1, value1, "", makets(0, 1))
	assert(ok and r == value1)
end, create_db, destroy_db)

test("TestMVCCResolveTxn", function (db)
	local v
	db:put(test_key1, value1, makets(0, 1), txn1)
	v = db:get(test_key1, makets(0, 1), txn1)
	assert(v == value1)
	local ok, r = pcall(db.get, db, test_key1, makets(0, 1))
	assert((not ok) and r:is('mvcc') and r.args[1] == 'txn_exists')
	-- Resolve will write with txn1's timestamp which is 1, 0
	db:resolve_txn(test_key1, #test_key1, makets(0, 1), txn1_commit)
	-- now non-transactional get can see the value put at 1, 0
	v = db:get(test_key1, makets(0, 1))
	assert(v == value1)
end, create_db, destroy_db)

test("TestMVCCAbortTxn", function (db)
	db:put(test_key1, value1, makets(0, 1), txn1)
	db:resolve_txn(test_key1, #test_key1, makets(0, 1), txn1_abort)

	assert(not db:get(test_key1, makets(1, 0)), "value should be empty")
	local meta_key = mvcc.make_key(test_key1, #test_key1)
	local v, vl = db.db:rawget(meta_key, #meta_key)
	assert(not db.db:get(meta_key), "expected no more metadata")
end, create_db, destroy_db)

test("TestMVCCAbortTxnWithPreviousVersion", function (db)
	local fixtures, results = {
		{test_key1, value1, makets(1, 0)},
		{test_key1, value2, makets(0, 1)},
		{test_key1, value3, makets(0, 2), txn1},
	}
	for _, data in ipairs(fixtures) do
		db:put(unpack(data))
	end

	db:resolve_txn(test_key1, #test_key1, makets(0, 2), txn1_abort)

	local meta_key = mvcc.make_key(test_key1, #test_key1)
	local meta, ml = db.db:rawget(meta_key, #meta_key)
	assert(meta ~= ffi.NULL and ml == ffi.sizeof('luact_mvcc_metadata_t'))

	local v, ts = db:get(test_key1, makets(0, 3))
	assert(v == value2 and ts == makets(0, 1))
end, create_db, destroy_db)

test("TestMVCCWriteWithDiffTimestampsAndEpochs", function (db)
	local fixtures, results = {
		{test_key1, value1, makets(1, 0), txn1}, -- ok. initial write
		{test_key1, value2, makets(0, 1), txn1_1}, -- ok, greater ts and n_retry
		{test_key1, value1, makets(1, 0), txn1_1}, -- should ignore. smaller ts
		{test_key1, value1, makets(0, 1), txn1}, -- should ignore. smaller n_retry
		{test_key1, value3, makets(0, 1), txn1_1}, -- ok. greater n_retry, same ts
	}
	for _, data in ipairs(fixtures) do
		db:put(unpack(data))
	end

	local ok, r, v, ts
	-- Resolve the txn.
	db:resolve_txn(test_key1, #test_key1, makets(0, 1), maketxn(txn1_1_commit, makets(0, 2)))
	-- Now try writing an earlier intent--should get write too old error.
	ok, r = pcall(db.put, db, test_key1, value2, makets(1, 0), txn2)
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
test("TestMVCCReadWithDiffEpochs", function (db)
	-- Write initial value wihtout a txn.
	db:put(test_key1, value1, makets(1, 0))
	db:put(test_key1, value2, makets(0, 1), txn1)
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
test("TestMVCCReadWithPushedTimestamp", function(db)
	-- Start with epoch 1.
	db:put(test_key1, value1, makets(1, 0), txn1)
	db:resolve_txn(test_key1, #test_key1, makets(1, 0), maketxn(txn1, makets(0, 1)))
	-- Attempt to read using naive txn's previous timestamp.
	local v = db:get(test_key1, makets(1, 0), txn1)
	assert(v == value1)
end, create_db, destroy_db)

test("TestMVCCResolveWithDiffEpochs", function (db)
	db:put(test_key1, value1, makets(0, 1), txn1)
	db:put(test_key2, value2, makets(0, 1), txn1_1)
	local test_key2_next = test_key2..string.char(0)
	local num = db:end_txn(test_key1, #test_key1, test_key2_next, #test_key2_next, 2, makets(0, 1), txn1_1_commit)
	assert(num == 2)

	-- Verify key1 is empty, as resolution with epoch 2 would have
	-- aborted the epoch 1 intent.
	assert(not db:get(test_key1, makets(0, 1)))

	-- Key2 should be committed.
	local v = db:get(test_key2, makets(0, 1))
	assert(v == value2)
end, create_db, destroy_db)

test("TestMVCCResolveWithUpdatedTimestamp", function (db)
	local v
	db:put(test_key1, value1, makets(0, 1), txn1)
	v = db:get(test_key1, makets(1, 1), txn1)
	assert(v == value1)

	-- Resolve with a higher commit timestamp -- this should rewrite the
	-- intent when making it permanent.
	db:resolve_txn(test_key1, #test_key1, makets(1, 1), maketxn(txn1_commit, makets(1, 1)))

	v = db:get(test_key1, makets(0, 1))
	assert(not v)

	v = db:get(test_key1, makets(1, 1))	
	assert(v == value1)
end, create_db, destroy_db)

test("TestMVCCResolveWithPushedTimestamp", function (db)
	local ok, v
	db:put(test_key1, value1, makets(0, 1), txn1)
	v = db:get(test_key1, makets(0, 1), txn1)
	assert(v == value1)

	-- Resolve with a higher commit timestamp, but with still-pending transaction.
	-- This represents a straightforward push (i.e. from a read/write conflict).
	db:resolve_txn(test_key1, #test_key1, makets(1, 1), maketxn(txn1, makets(1, 1))[0])

	ok, v = pcall(db.get, db, test_key1, makets(1, 1))
	-- because test_key1 is not committed, just pushed, so read latest version without txn will be failure
	assert((not ok) and v:is('mvcc') and v.args[1] == 'txn_exists') 

	-- Can still fetch the value using txn1.
	v = db:get(test_key1, makets(1, 1), txn1)	
	assert(v == value1)
end, create_db, destroy_db)

test("TestMVCCResolveTxnNoOps", function (db)
	-- Resolve a non existent key; noop.
	local ok, r = pcall(db.resolve_txn, db, test_key1, #test_key1, makets(0, 1), txn1_commit)
	assert((not ok) and r:is('invalid'))

	-- Add key and resolve despite there being no intent.
	db:put(test_key1, value1, makets(0, 1))
	db:resolve_txn(test_key1, #test_key1, makets(0, 1), txn2_commit)

	-- Write intent and resolve with different txn.
	db:put(test_key1, value2, makets(0, 1), txn1)
	db:resolve_txn(test_key1, #test_key1, makets(0, 1), txn2_commit)
end, create_db, destroy_db)

test("TestMVCCResolveTxnRange", function (db)
	local fixtures, results = {
		{test_key1, value1, makets(0, 1), txn1},
		{test_key2, value2, makets(0, 1), nil},
		{test_key3, value3, makets(0, 1), txn2},
		{test_key4, value4, makets(0, 1), txn1},
	}
	local test_key4_next = test_key4..string.char(0)
	for _, data in ipairs(fixtures) do
		db:put(unpack(data))
	end

	local num = db:end_txn(test_key1, #test_key1, test_key4_next, #test_key4_next, 0, makets(0, 1), txn1_commit)
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

-- ]] --










--[[

func TestValidSplitKeys(t *testing.T) {
	testCases := []struct {
		key   proto.Key
		valid bool
	}{
		{proto.Key("\x00"), false},
		{proto.Key("\x00\x00"), false},
		{proto.Key("\x00\x00meta1"), false},
		{proto.Key("\x00\x00meta1\x00"), false},
		{proto.Key("\x00\x00meta1\xff"), false},
		{proto.Key("\x00\x00meta2"), true},
		{proto.Key("\x00\x00meta2\x00"), true},
		{proto.Key("\x00\x00meta2\xff"), true},
		{proto.Key("\x00\x00meta3"), true},
		{proto.Key("\x00\x01"), true},
		{proto.Key("\x00accs\xff"), true},
		{proto.Key("\x00acct"), true},
		{proto.Key("\x00acct\x00"), false},
		{proto.Key("\x00acct\xff"), false},
		{proto.Key("\x00accu"), true},
		{proto.Key("\x00perl"), true},
		{proto.Key("\x00perm"), true},
		{proto.Key("\x00perm\x00"), false},
		{proto.Key("\x00perm\xff"), false},
		{proto.Key("\x00pern"), true},
		{proto.Key("\x00zond"), true},
		{proto.Key("\x00zone"), true},
		{proto.Key("\x00zone\x00"), false},
		{proto.Key("\x00zone\xff"), false},
		{proto.Key("\x00zonf"), true},
		{proto.Key("\x01"), true},
		{proto.Key("a"), true},
		{proto.Key("\xff"), true},
	}

	for i, test := range testCases {
		if valid := IsValidSplitKey(test.key); valid != test.valid {
			t.Errorf("%d: expected %q valid %t; got %t", i, test.key, test.valid, valid)
		}
	}
}

func TestFindSplitKey(t *testing.T) {
	raftID := int64(1)
	engine := NewInMem(proto.Attributes{}, 1<<20)
	ms := &MVCCStats{}
	// Generate a series of KeyValues, each containing targetLength
	// bytes, writing key #i to (encoded) key #i through the MVCC
	// facility. Assuming that this translates roughly into same-length
	// values after MVCC encoding, the split key should hence be chosen
	// as the middle key of the interval.
	for i := 0; i < splitReservoirSize; i++ {
		k := fmt.Sprintf("%09d", i)
		v := strings.Repeat("X", 10-len(k))
		val := proto.Value{Bytes: []byte(v)}
		// Write the key and value through MVCC
		if err := MVCCPut(engine, ms, []byte(k), makeTS(0, 1), val, nil); err != nil {
			t.Fatal(err)
		}
	}
	ms.MergeStats(engine, raftID) // write stats
	snap := engine.NewSnapshot()
	defer snap.Stop()
	humanSplitKey, err := MVCCFindSplitKey(snap, raftID, KeyMin, KeyMax)
	if err != nil {
		t.Fatal(err)
	}
	ind, _ := strconv.Atoi(string(humanSplitKey))
	if diff := splitReservoirSize/2 - ind; diff > 1 || diff < -1 {
		t.Fatalf("wanted key #%d+-1, but got %d (diff %d)", ind+diff, ind, diff)
	}
}

// TestFindValidSplitKeys verifies split keys are located such that
// they avoid splits through invalid key ranges.
func TestFindValidSplitKeys(t *testing.T) {
	raftID := int64(1)
	testCases := []struct {
		keys     []proto.Key
		expSplit proto.Key
		expError bool
	}{
		// All meta1 cannot be split.
		{
			keys: []proto.Key{
				proto.Key("\x00\x00meta1"),
				proto.Key("\x00\x00meta1\x00"),
				proto.Key("\x00\x00meta1\xff"),
			},
			expSplit: nil,
			expError: true,
		},
		// All zone cannot be split.
		{
			keys: []proto.Key{
				proto.Key("\x00zone"),
				proto.Key("\x00zone\x00"),
				proto.Key("\x00zone\xff"),
			},
			expSplit: nil,
			expError: true,
		},
		// Between meta1 and meta2, splits at meta2.
		{
			keys: []proto.Key{
				proto.Key("\x00\x00meta1"),
				proto.Key("\x00\x00meta1\x00"),
				proto.Key("\x00\x00meta1\xff"),
				proto.Key("\x00\x00meta2"),
				proto.Key("\x00\x00meta2\x00"),
				proto.Key("\x00\x00meta2\xff"),
			},
			expSplit: proto.Key("\x00\x00meta2"),
			expError: false,
		},
		// Even lopsided, always split at meta2.
		{
			keys: []proto.Key{
				proto.Key("\x00\x00meta1"),
				proto.Key("\x00\x00meta1\x00"),
				proto.Key("\x00\x00meta1\xff"),
				proto.Key("\x00\x00meta2"),
			},
			expSplit: proto.Key("\x00\x00meta2"),
			expError: false,
		},
		// Lopsided, truncate non-zone prefix.
		{
			keys: []proto.Key{
				proto.Key("\x00zond"),
				proto.Key("\x00zone"),
				proto.Key("\x00zone\x00"),
				proto.Key("\x00zone\xff"),
			},
			expSplit: proto.Key("\x00zone"),
			expError: false,
		},
		// Lopsided, truncate non-zone suffix.
		{
			keys: []proto.Key{
				proto.Key("\x00zone"),
				proto.Key("\x00zone\x00"),
				proto.Key("\x00zone\xff"),
				proto.Key("\x00zonf"),
			},
			expSplit: proto.Key("\x00zonf"),
			expError: false,
		},
	}

	for i, test := range testCases {
		engine := NewInMem(proto.Attributes{}, 1<<20)
		ms := &MVCCStats{}
		val := proto.Value{Bytes: []byte(strings.Repeat("X", 10))}
		for _, k := range test.keys {
			if err := MVCCPut(engine, ms, []byte(k), makeTS(0, 1), val, nil); err != nil {
				t.Fatal(err)
			}
		}
		ms.MergeStats(engine, raftID) // write stats
		snap := engine.NewSnapshot()
		defer snap.Stop()
		rangeStart := test.keys[0]
		rangeEnd := test.keys[len(test.keys)-1].Next()
		splitKey, err := MVCCFindSplitKey(snap, raftID, rangeStart, rangeEnd)
		if test.expError {
			if err == nil {
				t.Errorf("%d: expected error", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("%d; unexpected error: %s", i, err)
			continue
		}
		if !splitKey.Equal(test.expSplit) {
			t.Errorf("%d: expected split key %q; got %q", i, test.expSplit, splitKey)
		}
	}
}

// TestFindBalancedSplitKeys verifies split keys are located such that
// the left and right halves are equally balanced.
func TestFindBalancedSplitKeys(t *testing.T) {
	raftID := int64(1)
	testCases := []struct {
		keySizes []int
		valSizes []int
		expSplit int
	}{
		// Bigger keys on right side.
		{
			keySizes: []int{10, 100, 10, 10, 500},
			valSizes: []int{1, 1, 1, 1, 1},
			expSplit: 4,
		},
		// Bigger keys on left side.
		{
			keySizes: []int{1000, 100, 500, 10, 10},
			valSizes: []int{1, 1, 1, 1, 1},
			expSplit: 1,
		},
		// Bigger values on right side.
		{
			keySizes: []int{1, 1, 1, 1, 1},
			valSizes: []int{10, 100, 10, 10, 500},
			expSplit: 4,
		},
		// Bigger values on left side.
		{
			keySizes: []int{1, 1, 1, 1, 1},
			valSizes: []int{1000, 100, 500, 10, 10},
			expSplit: 1,
		},
		// Bigger key/values on right side.
		{
			keySizes: []int{10, 100, 10, 10, 250},
			valSizes: []int{10, 100, 10, 10, 250},
			expSplit: 4,
		},
		// Bigger key/values on left side.
		{
			keySizes: []int{500, 50, 250, 10, 10},
			valSizes: []int{500, 50, 250, 10, 10},
			expSplit: 1,
		},
	}

	for i, test := range testCases {
		engine := NewInMem(proto.Attributes{}, 1<<20)
		ms := &MVCCStats{}
		var expKey proto.Key
		for j, keySize := range test.keySizes {
			key := proto.Key(fmt.Sprintf("%d%s", j, strings.Repeat("X", keySize)))
			if test.expSplit == j {
				expKey = key
			}
			val := proto.Value{Bytes: []byte(strings.Repeat("X", test.valSizes[j]))}
			if err := MVCCPut(engine, ms, key, makeTS(0, 1), val, nil); err != nil {
				t.Fatal(err)
			}
		}
		ms.MergeStats(engine, raftID) // write stats
		snap := engine.NewSnapshot()
		defer snap.Stop()
		splitKey, err := MVCCFindSplitKey(snap, raftID, proto.Key("\x01"), proto.KeyMax)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
			continue
		}
		if !splitKey.Equal(expKey) {
			t.Errorf("%d: expected split key %q; got %q", i, expKey, splitKey)
		}
	}
}

// encodedSize returns the encoded size of the protobuf message.
func encodedSize(msg gogoproto.Message, t *testing.T) int64 {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return int64(len(data))
}

func verifyStats(debug string, ms *MVCCStats, expMS *MVCCStats, t *testing.T) {
	// ...And verify stats.
	if ms.LiveBytes != expMS.LiveBytes {
		t.Errorf("%s: mvcc live bytes %d; expected %d", debug, ms.LiveBytes, expMS.LiveBytes)
	}
	if ms.KeyBytes != expMS.KeyBytes {
		t.Errorf("%s: mvcc keyBytes %d; expected %d", debug, ms.KeyBytes, expMS.KeyBytes)
	}
	if ms.ValBytes != expMS.ValBytes {
		t.Errorf("%s: mvcc valBytes %d; expected %d", debug, ms.ValBytes, expMS.ValBytes)
	}
	if ms.IntentBytes != expMS.IntentBytes {
		t.Errorf("%s: mvcc intentBytes %d; expected %d", debug, ms.IntentBytes, expMS.IntentBytes)
	}
	if ms.LiveCount != expMS.LiveCount {
		t.Errorf("%s: mvcc liveCount %d; expected %d", debug, ms.LiveCount, expMS.LiveCount)
	}
	if ms.KeyCount != expMS.KeyCount {
		t.Errorf("%s: mvcc keyCount %d; expected %d", debug, ms.KeyCount, expMS.KeyCount)
	}
	if ms.ValCount != expMS.ValCount {
		t.Errorf("%s: mvcc valCount %d; expected %d", debug, ms.ValCount, expMS.ValCount)
	}
	if ms.IntentCount != expMS.IntentCount {
		t.Errorf("%s: mvcc intentCount %d; expected %d", debug, ms.IntentCount, expMS.IntentCount)
	}
	if ms.IntentAge != expMS.IntentAge {
		t.Errorf("%s: mvcc intentAge %d; expected %d", debug, ms.IntentAge, expMS.IntentAge)
	}
	if ms.GCBytesAge != expMS.GCBytesAge {
		t.Errorf("%s: mvcc gcBytesAge %d; expected %d", debug, ms.GCBytesAge, expMS.GCBytesAge)
	}
}

// TestMVCCStatsBasic writes a value, then deletes it as an intent via
// a transaction, then resolves the intent, manually verifying the
// mvcc stats at each step.
func TestMVCCStatsBasic(t *testing.T) {
	engine := createTestEngine()
	ms := &MVCCStats{}

	// Put a value.
	ts := makeTS(1*1E9, 0)
	key := proto.Key("a")
	value := proto.Value{Bytes: []byte("value")}
	if err := MVCCPut(engine, ms, key, ts, value, nil); err != nil {
		t.Fatal(err)
	}
	mKeySize := int64(len(MVCCEncodeKey(key)))
	mValSize := encodedSize(&proto.MVCCMetadata{Timestamp: ts}, t)
	vKeySize := int64(len(MVCCEncodeVersionKey(key, ts)))
	vValSize := encodedSize(&proto.MVCCValue{Value: &value}, t)

	expMS := &MVCCStats{
		LiveBytes: mKeySize + mValSize + vKeySize + vValSize,
		LiveCount: 1,
		KeyBytes:  mKeySize + vKeySize,
		KeyCount:  1,
		ValBytes:  mValSize + vValSize,
		ValCount:  1,
	}
	verifyStats("after put", ms, expMS, t)

	// Delete the value using a transaction.
	txn := &proto.Transaction{ID: []byte("txn1"), Timestamp: makeTS(1*1E9, 0)}
	ts2 := makeTS(2*1E9, 0)
	if err := MVCCDelete(engine, ms, key, ts2, txn); err != nil {
		t.Fatal(err)
	}
	m2ValSize := encodedSize(&proto.MVCCMetadata{Timestamp: ts2, Deleted: true, Txn: txn}, t)
	v2KeySize := int64(len(MVCCEncodeVersionKey(key, ts2)))
	v2ValSize := encodedSize(&proto.MVCCValue{Deleted: true}, t)
	expMS2 := &MVCCStats{
		KeyBytes:    mKeySize + vKeySize + v2KeySize,
		KeyCount:    1,
		ValBytes:    m2ValSize + vValSize + v2ValSize,
		ValCount:    2,
		IntentBytes: v2KeySize + v2ValSize,
		IntentCount: 1,
		IntentAge:   0,
		GCBytesAge:  vValSize + vKeySize, // immediately recognizes GC'able bytes from old value
	}
	verifyStats("after delete", ms, expMS2, t)

	// Resolve the deletion by aborting it.
	txn.Status = proto.ABORTED
	if err := MVCCResolveWriteIntent(engine, ms, key, ts2, txn); err != nil {
		t.Fatal(err)
	}
	// Stats should equal same as before the deletion after aborting the intent.
	verifyStats("after abort", ms, expMS, t)

	// Re-delete, but this time, we're going to commit it.
	txn.Status = proto.PENDING
	ts3 := makeTS(3*1E9, 0)
	if err := MVCCDelete(engine, ms, key, ts3, txn); err != nil {
		t.Fatal(err)
	}
	// GCBytesAge will be x2 seconds now.
	expMS2.GCBytesAge = (vValSize + vKeySize) * 2
	verifyStats("after 2nd delete", ms, expMS2, t) // should be same as before.

	// Put another intent, which should age deleted intent.
	ts4 := makeTS(4*1E9, 0)
	txn.Timestamp = ts4
	key2 := proto.Key("b")
	value2 := proto.Value{Bytes: []byte("value")}
	if err := MVCCPut(engine, ms, key2, ts4, value2, txn); err != nil {
		t.Fatal(err)
	}
	mKey2Size := int64(len(MVCCEncodeKey(key2)))
	mVal2Size := encodedSize(&proto.MVCCMetadata{Timestamp: ts4, Txn: txn}, t)
	vKey2Size := int64(len(MVCCEncodeVersionKey(key2, ts4)))
	vVal2Size := encodedSize(&proto.MVCCValue{Value: &value2}, t)
	expMS3 := &MVCCStats{
		KeyBytes:    mKeySize + vKeySize + v2KeySize + mKey2Size + vKey2Size,
		KeyCount:    2,
		ValBytes:    m2ValSize + vValSize + v2ValSize + mVal2Size + vVal2Size,
		ValCount:    3,
		LiveBytes:   mKey2Size + vKey2Size + mVal2Size + vVal2Size,
		LiveCount:   1,
		IntentBytes: v2KeySize + v2ValSize + vKey2Size + vVal2Size,
		IntentCount: 2,
		GCBytesAge:  (vValSize + vKeySize) * 2,
	}
	verifyStats("after 2nd put", ms, expMS3, t)

	// Now commit both values.
	txn.Status = proto.COMMITTED
	if err := MVCCResolveWriteIntent(engine, ms, key, ts4, txn); err != nil {
		t.Fatal(err)
	}
	if err := MVCCResolveWriteIntent(engine, ms, key2, ts4, txn); err != nil {
		t.Fatal(err)
	}
	m3ValSize := encodedSize(&proto.MVCCMetadata{Timestamp: ts4, Deleted: true}, t)
	m2Val2Size := encodedSize(&proto.MVCCMetadata{Timestamp: ts4}, t)
	expMS4 := &MVCCStats{
		KeyBytes:   mKeySize + vKeySize + v2KeySize + mKey2Size + vKey2Size,
		KeyCount:   2,
		ValBytes:   m3ValSize + vValSize + v2ValSize + m2Val2Size + vVal2Size,
		ValCount:   3,
		LiveBytes:  mKey2Size + vKey2Size + m2Val2Size + vVal2Size,
		LiveCount:  1,
		IntentAge:  -1,
		GCBytesAge: (vValSize+vKeySize)*2 + (m3ValSize - m2ValSize), // subtract off difference in metas
	}
	verifyStats("after commit", ms, expMS4, t)

	// Write over existing value to create GC'able bytes.
	ts5 := makeTS(10*1E9, 0) // skip ahead 6s
	if err := MVCCPut(engine, ms, key2, ts5, value2, nil); err != nil {
		t.Fatal(err)
	}
	expMS5 := expMS4
	expMS5.KeyBytes += vKey2Size
	expMS5.ValBytes += vVal2Size
	expMS5.ValCount = 4
	expMS5.GCBytesAge += (vKey2Size + vVal2Size) * 6 // since we skipped ahead 6s
	verifyStats("after overwrite", ms, expMS5, t)
}

// TestMVCCStatsWithRandomRuns creates a random sequence of puts,
// deletes and delete ranges and at each step verifies that the mvcc
// stats match a manual computation of range stats via a scan of the
// underlying engine.
func TestMVCCStatsWithRandomRuns(t *testing.T) {
	var seed int64
	err := binary.Read(crypto_rand.Reader, binary.LittleEndian, &seed)
	if err != nil {
		t.Fatalf("could not read from crypto/rand: %s", err)
	}
	log.Infof("using pseudo random number generator with seed %d", seed)
	rng := rand.New(rand.NewSource(seed))
	engine := createTestEngine()
	ms := &MVCCStats{}

	// Now, generate a random sequence of puts, deletes and resolves.
	// Each put and delete may or may not involve a txn. Resolves may
	// either commit or abort.
	keys := map[int32][]byte{}
	for i := int32(0); i < int32(1000); i++ {
		// Manually advance aggregate intent age based on one extra second of simulation.
		ms.IntentAge += ms.IntentCount
		// Same for aggregate gc'able bytes age.
		ms.GCBytesAge += ms.KeyBytes + ms.ValBytes - ms.LiveBytes

		key := []byte(fmt.Sprintf("%s-%d", util.RandString(rng, int(rng.Int31n(32))), i))
		keys[i] = key
		var txn *proto.Transaction
		if rng.Int31n(2) == 0 { // create a txn with 50% prob
			txn = &proto.Transaction{ID: []byte(fmt.Sprintf("txn-%d", i)), Timestamp: makeTS(int64(i+1)*1E9, 0)}
		}
		// With 25% probability, put a new value; otherwise, delete an earlier
		// key. Because an earlier step in this process may have itself been
		// a delete, we could end up deleting a non-existent key, which is good;
		// we don't mind testing that case as well.
		isDelete := rng.Int31n(4) == 0
		if i > 0 && isDelete {
			idx := rng.Int31n(i)
			log.V(1).Infof("*** DELETE index %d", idx)
			if err := MVCCDelete(engine, ms, keys[idx], makeTS(int64(i+1)*1E9, 0), txn); err != nil {
				// Abort any write intent on an earlier, unresolved txn.
				if wiErr, ok := err.(*proto.WriteIntentError); ok {
					wiErr.Txn.Status = proto.ABORTED
					log.V(1).Infof("*** ABORT index %d", idx)
					if err := MVCCResolveWriteIntent(engine, ms, keys[idx], makeTS(int64(i+1)*1E9, 0), &wiErr.Txn); err != nil {
						t.Fatal(err)
					}
					// Now, re-delete.
					log.V(1).Infof("*** RE-DELETE index %d", idx)
					if err := MVCCDelete(engine, ms, keys[idx], makeTS(int64(i+1)*1E9, 0), txn); err != nil {
						t.Fatal(err)
					}
				} else {
					t.Fatal(err)
				}
			}
		} else {
			rngVal := proto.Value{Bytes: []byte(util.RandString(rng, int(rng.Int31n(128))))}
			log.V(1).Infof("*** PUT index %d; TXN=%t", i, txn != nil)
			if err := MVCCPut(engine, ms, key, makeTS(int64(i+1)*1E9, 0), rngVal, txn); err != nil {
				t.Fatal(err)
			}
		}
		if !isDelete && txn != nil && rng.Int31n(2) == 0 { // resolve txn with 50% prob
			txn.Status = proto.COMMITTED
			if rng.Int31n(10) == 0 { // abort txn with 10% prob
				txn.Status = proto.ABORTED
			}
			log.V(1).Infof("*** RESOLVE index %d; COMMIT=%t", i, txn.Status == proto.COMMITTED)
			if err := MVCCResolveWriteIntent(engine, ms, key, makeTS(int64(i+1)*1E9, 0), txn); err != nil {
				t.Fatal(err)
			}
		}

		// Every 10th step, verify the stats via manual engine scan.
		if i%10 == 0 {
			// Compute the stats manually.
			expMS, err := MVCCComputeStats(engine, KeyMin, KeyMax, int64(i+1)*1E9)
			if err != nil {
				t.Fatal(err)
			}
			verifyStats(fmt.Sprintf("cycle %d", i), ms, &expMS, t)
		}
	}
}

// TestMVCCGarbageCollect writes a series of gc'able bytes and then
// sends an MVCC GC request and verifies cleared values and updated
// stats.
func TestMVCCGarbageCollect(t *testing.T) {
	engine := createTestEngine()
	ms := &MVCCStats{}

	bytes := []byte("value")
	ts1 := makeTS(1E9, 0)
	ts2 := makeTS(2E9, 0)
	ts3 := makeTS(3E9, 0)
	val1 := proto.Value{Bytes: bytes, Timestamp: &ts1}
	val2 := proto.Value{Bytes: bytes, Timestamp: &ts2}
	val3 := proto.Value{Bytes: bytes, Timestamp: &ts3}

	testData := []struct {
		key       proto.Key
		vals      []proto.Value
		isDeleted bool // is the most recent value a deletion tombstone?
	}{
		{proto.Key("a"), []proto.Value{val1, val2}, false},
		{proto.Key("a-del"), []proto.Value{val1, val2}, true},
		{proto.Key("b"), []proto.Value{val1, val2, val3}, false},
		{proto.Key("b-del"), []proto.Value{val1, val2, val3}, true},
	}

	for i := 0; i < 3; i++ {
		// Manually advance aggregate gc'able bytes age based on one extra second of simulation.
		ms.GCBytesAge += ms.KeyBytes + ms.ValBytes - ms.LiveBytes

		for _, test := range testData {
			if i >= len(test.vals) {
				continue
			}
			for _, val := range test.vals[i : i+1] {
				if i == len(test.vals)-1 && test.isDeleted {
					if err := MVCCDelete(engine, ms, test.key, *val.Timestamp, nil); err != nil {
						t.Fatal(err)
					}
					continue
				}
				if err := MVCCPut(engine, ms, test.key, *val.Timestamp, val, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	kvsn, err := Scan(engine, MVCCEncodeKey(KeyMin), MVCCEncodeKey(KeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, kv := range kvsn {
		key, ts, _ := MVCCDecodeKey(kv.Key)
		log.V(1).Infof("%d: %q, ts=%s", i, key, ts)
	}

	keys := []proto.InternalGCRequest_GCKey{
		{Key: proto.Key("a"), Timestamp: ts1},
		{Key: proto.Key("a-del"), Timestamp: ts2},
		{Key: proto.Key("b"), Timestamp: ts1},
		{Key: proto.Key("b-del"), Timestamp: ts2},
	}
	if err := MVCCGarbageCollect(engine, ms, keys, ts3); err != nil {
		t.Fatal(err)
	}

	expEncKeys := []proto.EncodedKey{
		MVCCEncodeKey(proto.Key("a")),
		MVCCEncodeVersionKey(proto.Key("a"), ts2),
		MVCCEncodeKey(proto.Key("b")),
		MVCCEncodeVersionKey(proto.Key("b"), ts3),
		MVCCEncodeVersionKey(proto.Key("b"), ts2),
		MVCCEncodeKey(proto.Key("b-del")),
		MVCCEncodeVersionKey(proto.Key("b-del"), ts3),
	}
	kvs, err := Scan(engine, MVCCEncodeKey(KeyMin), MVCCEncodeKey(KeyMax), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != len(expEncKeys) {
		t.Fatalf("number of kvs %d != expected %d", len(kvs), len(expEncKeys))
	}
	for i, kv := range kvs {
		key, ts, _ := MVCCDecodeKey(kv.Key)
		log.Infof("%d: %q, ts=%s", i, key, ts)
		if !kv.Key.Equal(expEncKeys[i]) {
			t.Errorf("%d: expected key %q; got %q", expEncKeys[i], kv.Key)
		}
	}

	// Verify aggregated stats match computed stats after GC.
	expMS, err := MVCCComputeStats(engine, KeyMin, KeyMax, ts3.WallTime)
	if err != nil {
		t.Fatal(err)
	}
	verifyStats("verification", ms, &expMS, t)
}

// TestMVCCGarbageCollectNonDeleted verifies that the first value for
// a key cannot be GC'd if it's not deleted.
func TestMVCCGarbageCollectNonDeleted(t *testing.T) {
	engine := createTestEngine()
	bytes := []byte("value")
	ts1 := makeTS(1E9, 0)
	ts2 := makeTS(2E9, 0)
	val1 := proto.Value{Bytes: bytes, Timestamp: &ts1}
	val2 := proto.Value{Bytes: bytes, Timestamp: &ts2}
	key := proto.Key("a")
	vals := []proto.Value{val1, val2}
	for _, val := range vals {
		if err := MVCCPut(engine, nil, key, *val.Timestamp, val, nil); err != nil {
			t.Fatal(err)
		}
	}
	keys := []proto.InternalGCRequest_GCKey{
		{Key: proto.Key("a"), Timestamp: ts2},
	}
	if err := MVCCGarbageCollect(engine, nil, keys, ts2); err == nil {
		t.Fatal("expected error garbage collecting a non-deleted live value")
	}
}

// TestMVCCGarbageCollectIntent verifies that an intent cannot be GC'd.
func TestMVCCGarbageCollectIntent(t *testing.T) {
	engine := createTestEngine()
	bytes := []byte("value")
	ts1 := makeTS(1E9, 0)
	ts2 := makeTS(2E9, 0)
	val1 := proto.Value{Bytes: bytes, Timestamp: &ts1}
	key := proto.Key("a")
	if err := MVCCPut(engine, nil, key, ts1, val1, nil); err != nil {
		t.Fatal(err)
	}
	txn := &proto.Transaction{ID: []byte("txn"), Timestamp: ts2}
	if err := MVCCDelete(engine, nil, key, ts2, txn); err != nil {
		t.Fatal(err)
	}
	keys := []proto.InternalGCRequest_GCKey{
		{Key: proto.Key("a"), Timestamp: ts2},
	}
	if err := MVCCGarbageCollect(engine, nil, keys, ts2); err == nil {
		t.Fatal("expected error garbage collecting an intent")
	}
}

]]
end)


return true
