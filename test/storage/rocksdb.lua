local luact = require 'luact.init'
local memory = require 'pulpo.memory'
local ffi = require 'ffiex.init'
local fs = require 'pulpo.fs'

local luact = require 'luact.init'

luact.start({
	datadir = "/tmp/luact",
	n_core = 1, exclusive = true,
}, function ()
local luact = require 'luact.init'
local fs = require 'pulpo.fs'
local ok,r = xpcall(function ()
	local ffi = require 'ffiex.init'
	local memory = require 'pulpo.memory'
	local rocksdb = require 'luact.storage.rocksdb'

	ffi.cdef [[
	typedef union keygen {
		uint64_t ll;
		uint8_t p[8];
	} keygen_t;
	typedef union valgen {
		uint32_t u[4];
		uint8_t p[16];
	} valgen_t;
	]]

	fs.rmdir('/tmp/luact/rocksdb/testdb')
	fs.mkdir('/tmp/luact/rocksdb/testdb')
	local db = rocksdb.open('/tmp/luact/rocksdb/testdb', nil, {
		initial_column_family_buffer_size = 1,
	})
	local p = memory.alloc_typed('valgen_t')
	for i=1,4 do
		p.u[i - 1] = 0xdeadbeef
	end

	local keygen = ffi.new('keygen_t')

	for i=1,1000,1 do
		keygen.ll = i
		db:rawput(keygen.p, 8, p.p, 16)
	end

	for i=1,1000,1 do
		keygen.ll = i
		local v, vl = db:rawget(keygen.p, 8)
		assert(vl == 16, "data length should be equal to put size")
		for i=1,4 do
			assert(ffi.cast('uint32_t*', v)[i - 1] == 0xdeadbeef, "data contents should never change")
		end
	end

	local txn_key_start = 10000000
	local txn = db:new_txn()
	for i=txn_key_start,txn_key_start+100 do
		keygen.ll = i
		txn:rawput(keygen.p, 8, p.p, 16)
	end
	txn = nil -- abort
	for i=txn_key_start,txn_key_start+100 do
		keygen.ll = i
		local ptr, pl = db:rawget(keygen.p, 8)
		-- print('aftabort', ptr, pl, ffi.string(ptr, pl))
		assert(ffi.NULL == ptr, "if transaction is not committed, put key should not appear")
	end

	txn = db:new_txn()
	for i=txn_key_start,txn_key_start+100 do
		keygen.ll = i
		txn:rawput(keygen.p, 8, p.p, 16)
	end
	txn:commit()
	for i=txn_key_start,txn_key_start+100 do
		keygen.ll = i
		local ptr, pl = db:rawget(keygen.p, 8)
		-- print('aftcommit', ptr, pl)
		assert(ffi.NULL ~= ptr and pl == 16, "if transaction is committed, put key should appear")
		for i=1,4 do
			assert(ffi.cast('uint32_t*', ptr)[i - 1] == 0xdeadbeef, "data contents should never change")
		end
	end

	txn = db:new_txn()
	for i=txn_key_start,txn_key_start+100 do
		keygen.ll = i
		txn:rawdelete(keygen.p, 8)
	end
	txn:commit()
	for i=txn_key_start,txn_key_start+100 do
		keygen.ll = i
		local ptr, pl = db:rawget(keygen.p, 8)
		-- print('aftabort', ptr, pl, ffi.string(ptr, pl))
		assert(ffi.NULL == ptr, "if key is deleted, put key should not appear")
	end


	local cf1, cf2 = db:column_family('hoge'), db:column_family('fuga')
	local result_buffer1, result_buffer2 = ffi.new('bool[1]'), ffi.new('bool[1]')
	cf1:put('key', 'val')
	cf2:put('key', 'value')
	assert(cf1:get('key') == 'val', "column family should store key value pair correctly")
	assert(cf2:get('key') == 'value', "column family should store key value pair correctly")

	cf1:merge('key', rocksdb.op_cas('val', 'value', result_buffer1))
	assert(cf1:get('key') == 'value' and result_buffer1[0], "cas change value correctly")

	cf1:merge('key', rocksdb.op_cas('value', 'val', result_buffer1))
	assert(cf1:get('key') == 'val' and result_buffer1[0], "cas change value correctly")

	local sync_write_opts = rocksdb.new_write_opts({sync = 1})
	cf2:merge('not_found', rocksdb.op_cas(nil, 'var', result_buffer2), sync_write_opts)
	assert(cf2:get('not_found') == 'var' and result_buffer2[0], "cas with no value should success")
	
	cf1:fin()
	cf2:fin()
	db:close()

	-- reopen
	db = rocksdb.open('/tmp/luact/rocksdb/testdb')
	cf1, cf2 = db:column_family('hoge'), db:column_family('fuga')
	assert(cf1:get('key') == 'val', "column family should store key value pair correctly")
	assert(cf2:get('key') == 'value', "column family should store key value pair correctly")

	-- merge test
	result_buffer1[0], result_buffer2[0] = false, false
	cf1:put('merge_key', 'foo')
	cf2:put('merge_key', 'bar')
	cf1:merge('merge_key', rocksdb.op_cas('foo', 'bar', result_buffer1))
	cf2:merge('merge_key', rocksdb.op_cas('foo', 'bar', result_buffer2))
	assert((cf1:get('merge_key') == 'bar') and result_buffer1[0], 
		"cf1 cas meets comparison condition, so value should change")
	assert((cf2:get('merge_key') == 'bar') and (not result_buffer2[0]), 
		"cf2 cas does not meet comparison condition, so value should not change")
	cf1:merge('merge_key_not_exist', rocksdb.op_cas('foo', 'bar', result_buffer1))
	cf2:merge('merge_key_not_exist', rocksdb.op_cas(nil, 'bar', result_buffer2))
	assert((not cf1:get('merge_key_not_exist')) and (not result_buffer1[0]), 
		"cas for non-existent key should match with nil value, so value should not set")
	assert((cf2:get('merge_key_not_exist') == 'bar') and result_buffer2[0], 
		"cas for non-existent key should match with nil value, so value should set")

	-- iterator test
	local cf3 = db:column_family('test_iter')
	for i=("z"):byte(),("a"):byte(),-1 do
		cf3:put((string.char(i)):rep(16), "fuga")
	end
	
	local iter = cf3:iterator()
	iter:last()
	iter:first()
	iter:seek("not found", #("not found")) -- error
	assert(iter:keystr() == ("o"):rep(16), "if not exist, should seek to smallest of bigger key")
	iter:seek(("p"):rep(16), 16)
	local cnt = ("p"):byte()
	while iter:valid() do
		assert(iter:valstr() == "fuga")
		assert(iter:keystr() == (string.char(cnt)):rep(16))
		cnt = cnt + 1
		iter:next()
	end
	assert(cnt == (1 + ("z"):byte()))
end, function (e)
	logger.error(e, debug.traceback())
end)
	-- fs.rmdir('/tmp/luact/rocksdb/testdb')	

	luact.stop()
end)

return true




