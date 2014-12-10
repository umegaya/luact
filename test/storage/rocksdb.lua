local luact = require 'luact.init'
local memory = require 'pulpo.memory'
local ffi = require 'ffiex.init'
local fs = require 'pulpo.fs'

local luact = require 'luact.init'

luact.start({
	cache_dir = "/tmp/luact",
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
	cf1:put('key', 'val')
	cf2:put('key', 'value')
	assert(cf1:get('key') == 'val', "column family should store key value pair correctly")
	assert(cf2:get('key') == 'value', "column family should store key value pair correctly")
	cf1:fin()
	cf2:fin()
	db:close()

	-- reopen
	db = rocksdb.open('/tmp/luact/rocksdb/testdb')
	cf1, cf2 = db:column_family('hoge'), db:column_family('fuga')
	assert(cf1:get('key') == 'val', "column family should store key value pair correctly")
	assert(cf2:get('key') == 'value', "column family should store key value pair correctly")

end, function (e)
	logger.error('err', e, debug.traceback())
end)
	-- fs.rmdir('/tmp/luact/rocksdb/testdb')	

	luact.stop()
end)

return true




