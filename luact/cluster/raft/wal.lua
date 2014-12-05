local ffi = require 'ffiex.init'
local C = ffi.C

local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local pbuf = require 'pulpo.pbuf'
local fs = require 'pulpo.fs'

local ringbuf = require 'luact.cluster.raft.ringbuf'

local _M = {}

-- cdefs
ffi.cdef [[
typedef struct luact_raft_wal_writer {
	luact_rbuf_t rb;
	int fd;
	uint64_t last_index;
} luact_raft_wal_writer_t;
]]


-- luact_raft_wal_writer_t
local wal_writer_index = {}
local wal_writer_mt = {
	__index = wal_writer_index
}
function wal_writer_index:init()
	self.rb:init()
	self:open(dir)
end
function wal_writer_index:fin()
	self.rb:fin()
	if self.fd >= 0 then
		C.close(self.fd)
	end
end
function wal_writer_index:compaction(store, upto_idx)
	store:compaction(upto_idx)
end
function wal_writer_index:new_log(kind, term, index, logbody)
	return { kind = kind, term = term, index = index, log = logbody }
end
function wal_writer_index:write(store, kind, term, logs, serde, logcache, msgid)
	local last_index = self.last_index
	for i=1,#logs,1 do
		last_index = last_index + 1
		local log = self:new_log(kind, term, last_index, logs[i])
		-- last log have coroutine object to resume after these logs are accepted.
		-- (its not necessary for persisted data, so after serde:pack)
		if i == #logs and msgid then log.msgid = msgid end
		logcache:put_at(last_index, log)
	end
	local ok, r = store:put_logs(logcache, serde, self.last_index, last_index)
	if not ok then
		exception.raise('fatal', 'raft', 'fail to commit logs', r)
	end
	self.last_index = last_index
	return self.last_index
end
function wal_writer_index:delete(store, start_idx, end_idx)
	store:delete_logs(start_idx, end_idx)
end
-- following 3 are using for writing multi-kind of data (hardstate, replica_set)
function wal_writer_index:write_state(store, kind, state, serde, logcache)
	local fd = self:open_state_file(path)
	store:put_object(kind, serde, state)
end
function wal_writer_index:read_state(store, kind, serde, logcache)
	return store:get_object(kind, serde)
end
function wal_writer_index:restore(store, serde, index, fsm, metadata)
	local verified
	local term, data
	local rb = self.rb
	while true do
		rb:reset()
		local obj = store:get_log(index, serde)
		if not obj then 
			exception.raise('fatal', 'cannot load log', index)
		end
		if not verified then
			local ok, r = metadata:verify(obj)
			if not ok then
				exception.raise('fatal', 'invalid metadata', r)
			end
			verified = true
		end
		term, index, data = obj.term, obj.index, obj.log
		if (self.last_index > 0) and (index - self.last_index) ~= 1 then
			exception.raise('fatal', 'invalid index leap', self.last_index, index)
		end				
		local ok, r = pcall(fsm.apply, fsm, data)
		if not ok then
			exception.raise('fatal', 'fail to apply log to fsm', r)
		end
		self.last_index = index
		index = index + 1
	end
	return term, self.last_index
end
ffi.metatype('luact_raft_wal_writer_t', wal_writer_mt)

-- wal object 
local wal_index = {}
local wal_mt = {
	__index = wal_index
}
function wal_index:init()
	self.writer:init()
end
function wal_index:fin()
	self.writer:fin()
	self.store:fin()
end
function wal_index:restore(index, fsm)
	self.writer:restore(self.store, self.serde, index, fsm, self.meta)
end
function wal_index:compaction(upto_idx)
	-- remove in memory log with some margin (because minority node which hasn't replicate old log exists.)
	if upto_idx > self.opts.log_compaction_margin then
		self.writer:compaction(self.dir, upto_idx - self.opts.log_compaction_margin)
	end
end
function wal_index:write(kind, term, logs, msgid)
	if not pcall(self.writer.write, self.writer, self.store, kind, term, logs, self.serde, self.logcache, msgid) then
		self.store:rollback()
	end
end
function wal_index:delete(start_idx, end_idx)
	return self.writer:delete(start_idx, end_idx)
end
function wal_index:read_state()
	return self.writer:read_state(self.store, self.serde, self.logcache)
end
function wal_index:write_state(state)
	self.writer:write_state(self.store, state, self.serde, self.logcache)
end
function wal_index:read_replica_set(replica_set)
	return self.writer:read_state(self.store, self.serde, self.logcache)
end
function wal_index:write_replica_set(replica_set)
	self.writer:write_state(self.store, replica_set, self.serde, self.logcache)
end
function wal_index:restore_state()
	return self.writer:restore_state(self.store, self.serde)
end
function wal_index:at(idx)
	return self.logcache:at(idx)
end
function wal_index:logs_from(start_idx)
	return self.logcache:from(start_idx)
end
function wal_index:delete_logs(start_idx, end_idx)
	return self.writer:delete(self.store, start_idx, end_idx)
end
function wal_index:last_index()
	return self.writer.last_index
end

-- module function
function _M.new(meta, store, serde, opts)
	local wal = setmetatable({
		writer = memory.alloc_typed('luact_raft_wal_writer_t'),
		store = store,
		-- TODO : using cdata array if serde is such kind.
		logcache = ringbuf.new(opts.log_compaction_margin)
		meta = meta, 
		serde = serde,
		opts = opts,
	}, wal_mt)
	wal:init()
	return wal
end

return _M
