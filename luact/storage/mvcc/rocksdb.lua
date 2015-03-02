local ffi = require 'ffiex.init'
local mvcc = require 'luact.storage.mvcc'
local rocksdb = require 'luact.storage.rocksdb'
local exception = require 'pulpo.exception'
local memory = require 'pulpo.memory'

local _M = {}


-- cdefs
ffi.cdef [[
typedef struct luact_mvcc_rocksdb {
	rocksdb_t *db;
} luact_mvcc_rocksdb_t;

typedef struct luact_mvcc_rocksdb_cf {
	luact_rocksdb_column_family_t *db;
} luact_mvcc_rocksdb_cf_t;
]]


-- local functions
local function new_mvcc(db)
	local p = memory.alloc_typed('luact_mvcc_rocksdb_t')
	p:init(db)
	return p
end
local function new_mvcc_cf(cf)
	local p = memory.alloc_typed('luact_mvcc_rocksdb_cf_t')
	p:init(cf)
	return p
end


-- mvcc rocksdb
local mvcc_rocksdb_mt = mvcc.new_mt()
function mvcc_rocksdb_mt:column_family(name, opts)
	return new_mvcc_cf(self.db:column_family(name, opts))
end
function mvcc_rocksdb_mt:iterator(opts)
	return self.db:iterator(opts)
end
function mvcc_rocksdb_mt:binpairs(opts)
	return self.db:binpairs(opts)
end
function mvcc_rocksdb_mt:pairs(opts)
	return self.db:pairs(opts)
end
ffi.metatype('luact_mvcc_rocksdb_t', mvcc_rocksdb_mt)

-- mvcc rocksdb column family
local mvcc_rocksdb_cf_mt = mvcc.new_mt()
function mvcc_rocksdb_cf_mt:column_family(name, opts)
	exception.raise('invalid', 'column_family cannot create column_family')
end
function mvcc_rocksdb_cf_mt:iterator(opts)
	return self.db:iterator(opts)
end
function mvcc_rocksdb_cf_mt:binpairs(opts)
	return self.db:binpairs(opts)
end
function mvcc_rocksdb_cf_mt:pairs(opts)
	return self.db:pairs(opts)
end
ffi.metatype('luact_mvcc_rocksdb_cf_t', mvcc_rocksdb_cf_mt)


-- module functions
function _M.open(name, opts, debug_conf)
	return new_mvcc(rocksdb.open(name, opts, debug_conf))
end
function _M.close(mvcc)
	mvcc:close()
end
function _M.make_key(k, kl, ts) 
	local key = mvcc.make_key(k, kl, ts)
	return key
end
_M.dump_key = mvcc.dump_key

return _M
