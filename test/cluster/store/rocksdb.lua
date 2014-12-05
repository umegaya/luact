local luact = require 'luact.init'
local memory = require 'pulpo.memory'
local ffi = require 'ffiex.init'

local luact = require 'luact.init'

luact.start({
	cache_dir = "/tmp/luact",
	n_core = 1, exclusive = true,
}, function ()
	local luact = require 'luact.init'
	local ffi = require 'ffiex.init'
	local fs = require 'pulpo.fs'
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

	fs.rmdir('/tmp/rocksdb')
	fs.mkdir('/tmp/rocksdb')
	local db = rocksdb.open('/tmp/rocksdb/testdb')
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
		assert(vl == 16)
		for i=1,4 do
			assert(ffi.cast('uint32_t*', v)[i - 1] == 0xdeadbeef)
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
		local ptr = db:rawget(keygen.p, 8)
		assert(ffi.NULL == ptr)
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
		assert(ffi.NULL ~= ptr and pl == 16)
		for i=1,4 do
			assert(ffi.cast('uint32_t*', ptr)[i - 1] == 0xdeadbeef)
		end
	end
	luact.stop()
end)

return true




