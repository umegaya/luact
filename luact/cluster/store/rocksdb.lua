local ffi = require 'ffiex.init'
local C = ffi.C

local pulpo = require 'pulpo.init'
local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local fs = require 'pulpo.fs'

local pbuf = require 'luact.pbuf'
local db = require 'luact.storage.rocksdb'

local _M = {}

-- cdefs
ffi.cdef [[
typedef struct luact_store_rocksdb {
	const char *name;
	rocksdb_t *db;
	uint64_t min_idx, max_idx;
	luact_rbuf_t rb;
} luact_store_rocksdb_t;
typedef union luact_logkey_gen {
	uint64_t ll;
	uint8_t p[8];
} luact_logkey_gen_t;
]]

-- luact_store_rocksdb
local store_rocksdb_index = {}
local store_rocksdb_mt = {
	__index = store_rocksdb_index
}
function store_rocksdb_index:init(dir, name, opts)
	self.name = memory.strdup(name)
	self.rb:init()
	self:open(dir, name, opts)
	self.min_idx = 0
	self.max_idx = 0
end
function store_rocksdb_index:fin()
	self.db:fin()
	self.rb:fin()
	memory.free(self.dir)
	memory.free(self.pfx)
end
local logkeygen = ffi.new('luact_logkey_gen_t')
local logkeysize = ffi.sizeof('luact_logkey_gen_t');
function store_rocksdb_index:logkey(idx)
	logkeygen.ll = idx
	return logkeygen.p, logkeysize
end
function store_rocksdb_index:state_key()
	return util.rawsprintf("%s/state", self.pfx)
end
function store_rocksdb_index:replica_set_key()
	return util.rawsprintf("%s/replicas", self.pfx)
end
function store_rocksdb_index:key_by_kind(kind)
	if kind == 'state' then
		return self:state_key()
	elseif kind == 'replica_set' then
		return self:replica_set_key()
	else
		exception.raise('invalid', 'objects type', kind)
	end
end
function store_rocksdb_index:column_family_name()
	return 'luact_raft_logs'..pulpo.thread_id
end
function store_rocksdb_index:open(dir, name, opts)
	local path = fs.path(dir, name)
	self.db = db.open(path, opts):column_family(self:column_family_name())
end

-- interfaces for consensus modules
function store_rocksdb_index:compact(upto_idx)
	self:delete_logs(self.min_idx, upto_idx)
end
function store_rocksdb_index:put_object(kind, serde, object)
	local k, kl = self:key_by_kind(kind)
	serde:pack(self.rb, object)
	self.db:rawput(k, kl, rb:start_p(), rb:available())
end
function store_rocksdb_index:get_object(kind, serde)
	local k, kl = self:key_by_kind(kind)
	local p, pl = self.db:rawget(k, kl)
	rb:reserve(pl)
	ffi.copy(rb:start_p(), p, pl)
	local obj, err = serde:unpack(rb)
	if err then
		exception.raise('invalid', 'object data', err)
	end
	return obj
end
function store_rocksdb_index:put_logs(logcache, serde, start_idx, end_idx)
	local txn = self.db:new_txn()
	local ok, r = pcall(self.unsafe_put_logs, self, txn, logcache, serde, start_idx, end_idx)
	if ok then
		txn:commit()
		if self.min_idx <= 0 then
			self.min_idx = start_idx
		end
		if self.max_idx < end_idx then
			self.max_idx = end_idx
		end
		return true
	end
	return nil, r
end
function store_rocksdb_index:unsafe_put_logs(txn, logcache, serde, start_idx, end_idx)
	for idx=start_idx,end_idx do
		local log = logcache:at(idx)
		self.rb:reset()
		serde:pack(self.rb, log)
		local k, kl = self:logkey(idx)
		txn:rawput(k, kl, rb:start_p(), rb:available())
	end
end
function store_rocksdb_index:delete_logs(start_idx, end_idx)
	local txn = self.db:new_txn()
	local ok, r = pcall(self.unsafe_delete_logs, self, txn, start_idx, end_idx) 
	if ok then
		txn:commit()
		if self.min_idx >= start_idx then
			self.min_idx = end_idx
		else
			logger.error('raft', 'it seems *bug* that delete_logs creates index gap', self.min_idx, start_idx)
		end
		return true
	end
	return nil, r
end
function store_rocksdb_index:unsafe_delete_logs(txn, start_idx, end_idx)
	for idx=start_idx,end_idx do
		txn:rawdelete(self:logkey(idx))
	end
end
function store_rocksdb_index:get_log(idx, serde)
	local p, pl = self.db:rawget(self:logkey(idx))
	self.rb:reset()
	self.rb:reserve(pl)
	ffi.copy(self.rb:start_p(), p, pl)
	return serde:unpack(self.rb)
end
ffi.metatype('luact_store_rocksdb_t', store_rocksdb_mt)



-- module functions
function _M.new(dir, name, opts)
	local p = memory.alloc_typed('luact_store_rocksdb_t')
	p:init(dir, name, opts)
	return p
end

return _M
