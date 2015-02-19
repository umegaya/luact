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
_M.MAX_DBNAME_LEN = 256

-- cdefs
ffi.cdef [[
typedef struct luact_store_rocksdb {
	char *name;
	luact_rocksdb_column_family_t *db;
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
	if util.strlen(self.name, _M.MAX_DBNAME_LEN + 1) >= (_M.MAX_DBNAME_LEN + 1) then
		exception.raise('invalid', 'dbname', _M.MAX_DBNAME_LEN)
	end
	self.rb:init()
	self:open(dir, name, opts)
	self.min_idx = 0
	self.max_idx = 0
end
function store_rocksdb_index:fin()
	self.db:fin()
	self.rb:fin()
	memory.free(self.name)
end
local logkeygen = ffi.new('luact_logkey_gen_t')
local logkeysize = ffi.sizeof('luact_logkey_gen_t');
local function keydump(p)
	print(p[0], p[1], p[2], p[3], 
		p[4], p[5], p[6], p[7])
end
function store_rocksdb_index:logkey(idx)
	logkeygen.ll = idx
	return logkeygen.p, logkeysize
end
function store_rocksdb_index:state_key()
	return util.rawsprintf("%s/state", _M.MAX_DBNAME_LEN + 6, self.name)
end
function store_rocksdb_index:metadata_key()
	return util.rawsprintf("%s/meta", _M.MAX_DBNAME_LEN + 5, self.name)
end
function store_rocksdb_index:key_by_kind(kind)
	if kind == 'state' then
		return self:state_key()
	elseif kind == 'metadata' then
		return self:metadata_key()
	else
		exception.raise('invalid', 'objects type', kind)
	end
end
function store_rocksdb_index:column_family_name(name)
	return 'luact_raft_logs'..tostring(pulpo.thread_id)..'_'..name
end
function store_rocksdb_index:open(dir, name, opts)
	fs.mkdir(dir)
	self.db = db.open(dir, opts):column_family(self:column_family_name(name))
end
function store_rocksdb_index:init_rbuf()
	self.rb:reset()
	return self.rb
end
function store_rocksdb_index:to_rbuf(p, pl)
	local rb = self:init_rbuf()
	-- print('to_rbuf:', pl, ffi.string(p, pl))
	rb:reserve(pl)
	-- remove this copy
	ffi.copy(rb:start_p(), p, pl)
	rb:use(pl)
	memory.free(p)
	return rb
end

-- interfaces for consensus modules
function store_rocksdb_index:compaction(upto_idx)
	logger.notice('compaction', upto_idx, self.min_idx)--, debug.traceback())
	if upto_idx >= self.min_idx then
		self:delete_logs(self.min_idx, upto_idx)
		-- print('aft compaction:', self.min_idx)
	end
end
function store_rocksdb_index:put_object(kind, serde, object)
	local k, kl = self:key_by_kind(kind)
	local rb = self:init_rbuf()
	local ok, r = pcall(serde.pack, serde, rb, object)
	if not ok then
		logger.report('storage', 'error', 'put_object', r)
		error(r)
	end
	self.db:rawput(k, kl, rb:start_p(), rb:available())
end
function store_rocksdb_index:get_object(kind, serde)
	local k, kl = self:key_by_kind(kind)
	local p, pl = self.db:rawget(k, kl)
	if p ~= util.NULL then
		local rb = self:to_rbuf(p, pl)
		local obj, err = serde:unpack(rb)
		if not obj then
			exception.raise('invalid', 'object data', err)
		end
		return obj
	else
		return nil
	end
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
	for idx=tonumber(start_idx),tonumber(end_idx) do
		local log = logcache:at(idx)
		if not log then
			exception.raise('not_found', 'log', idx)
		end
		local rb = self:init_rbuf()
		serde:pack(rb, log)
		local k, kl = self:logkey(idx)
		-- print('put_logs:', idx, ffi.string(rb:start_p(), rb:available()))
		txn:rawput_cf(self.db, k, kl, rb:start_p(), rb:available())
	end
end
function store_rocksdb_index:delete_logs(start_idx, end_idx)
	local txn = self.db:new_txn()
	start_idx = start_idx or self.min_idx
	if self.min_idx < start_idx and end_idx < self.max_idx then
		return nil, exception.new('invalid', 'it seems *bug* that delete_logs creates index gap', 
			self.min_idx, start_idx, self.max_idx, end_idx)
	else
		local ok, r = pcall(self.unsafe_delete_logs, self, txn, start_idx, end_idx) 
		if ok then
			if self.min_idx < start_idx and end_idx < self.max_idx then
				exception.raise('invalid', 'it seems *bug* that delete_logs creates index gap', 
					self.min_idx, start_idx, self.max_idx, end_idx)
			else
				if self.min_idx >= start_idx then
					self.min_idx = end_idx + 1
				end
				if self.max_idx <= end_idx then
					self.max_idx = start_idx - 1
				end
			end
			txn:commit()
			return self.max_idx
		end
		logger.error('raft', 'fail to delete_logs', r)
		return nil, r
	end
end
function store_rocksdb_index:unsafe_delete_logs(txn, start_idx, end_idx)
	for idx=tonumber(start_idx),tonumber(end_idx) do
		txn:rawdelete_cf(self.db, self:logkey(idx))
	end
end
function store_rocksdb_index:get_log(idx, serde)
	local p, pl = self.db:rawget(self:logkey(idx))
	if p == util.NULL then return nil end
	if _M.DEBUG then
		print('get_log', idx, ffi.string(p, pl))
	end
	local rb = self:to_rbuf(p, pl)
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
