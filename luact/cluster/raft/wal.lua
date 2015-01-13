local ffi = require 'ffiex.init'
local C = ffi.C

local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local fs = require 'pulpo.fs'

local pbuf = require 'luact.pbuf'
local ringbuf = require 'luact.util.ringbuf'

local _M = {}

-- cdefs
ffi.cdef [[
typedef struct luact_raft_wal_writer {
	luact_rbuf_t rb;
	int fd;
	uint64_t last_index, last_term;
} luact_raft_wal_writer_t;
]]


-- luact_raft_wal_writer_t
local wal_writer_index = {}
local wal_writer_mt = {
	__index = wal_writer_index
}
function wal_writer_index:init()
	self.rb:init()
	self.fd = -1
end
function wal_writer_index:fin()
	self.rb:fin()
	if self.fd >= 0 then
		C.close(self.fd)
	end
end
function wal_writer_index:new_log(kind, term, index, logbody)
	if type(kind) == 'table' then
		exception.raise('fatal')
	end
	return { kind = kind, term = term, index = index, log = logbody }
end
-- called from apply. set logical order and store logs
function wal_writer_index:write(store, kind, term, logs, serde, logcache, msgid)
	local last_index = self.last_index
	if term < self.last_term then
		exception.raise('invalid', 'log term back', self.last_term, log.term)
	end
	for i=1,#logs,1 do
		last_index = last_index + 1
		local log = self:new_log(kind, term, last_index, logs[i])
		-- last log have coroutine object to resume after these logs are accepted.
		-- (its not necessary for persisted data, so after serde:pack)
		if i == #logs and msgid then log.msgid = msgid end
		logcache:put_at(last_index, log)
		logger.info('writer', 'logat', last_index, logcache:at(last_index))--, debug.traceback())
	end
	-- logger.report('write:after put logcache')
	-- logcache:dump()
	local first_index = self.last_index + 1
	local ok, r = store:put_logs(logcache, serde, first_index, last_index)
	if not ok then
		logger.report('write wal fails', r)
		logcache:rollback_index(self.last_index)
		return nil
	end
	-- print('last_index:', self.last_index, "=>", last_index)
	self.last_index = last_index
	self.last_term = term
	return first_index, last_index
end
-- called from append entries. just copy received logs.
function wal_writer_index:copy(store, logs, serde, logcache)
	local last_index = self.last_index
	local last_term = self.last_term
	for i=1,#logs do
		local log = logs[i]
		if last_index > 0 and (log.index - last_index) ~= 1 then
			exception.raise('invalid', 'log index leap', last_index, log.index)
		end
		if last_term > log.term then
			exception.raise('invalid', 'log term back', last_term, log.term)
		end
		if log.msgid then log.msgid = nil end -- remove remote msgid
		last_index = log.index
		last_term = log.term
		logcache:put_at(last_index, log)
		logger.info('copy', 'logat', last_index, tostring(logcache:at(last_index)))
	end
	local first_index = self.last_index + 1
	local ok, r = store:put_logs(logcache, serde, first_index, last_index)
	if not ok then
		logger.report('copy wal fails', r)
		logcache:rollback_index(self.last_index)
		return nil
	end
	-- print('last_index:', self.last_index, "=>", last_index)
	self.last_index = last_index
	self.last_term = last_term
	return first_index, last_index
end
-- following 3 are using for writing multi-kind of data (hardstate, replica_set)
function wal_writer_index:write_state(store, kind, state, serde, logcache)
	store:put_object(kind, serde, state)
end
function wal_writer_index:read_state(store, kind, serde, logcache)
	return store:get_object(kind, serde)
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
	memory.free(self.writer)
	self.store:fin()
end
function wal_index:can_restore()
	local obj = self:read_metadata()
	return obj and util.table_equals(self.meta, obj)
end
function wal_index:compaction(upto_idx)
	-- remove in memory log with some margin (because minority node which hasn't replicate old log exists.)
	if upto_idx > self.opts.log_compaction_margin then
		self.store:compaction(upto_idx - self.opts.log_compaction_margin)
		self.logcache:delete_range(nil, upto_idx - self.opts.log_compaction_margin)
	else
		logger.warn('raft', 'upto_idx too short', upto_idx, self.opts.log_compaction_margin)
	end
end
function wal_index:write(kind, term, logs, msgid)
	return self.writer:write(self.store, kind, term, logs, self.serde, self.logcache, msgid)
end
function wal_index:copy(logs)
	return self.writer:copy(self.store, logs, self.serde, self.logcache)
end
function wal_index:read_state()
	return self.writer:read_state(self.store, 'state', self.serde, self.logcache)
end
function wal_index:write_state(state)
	self.writer:write_state(self.store, 'state', state, self.serde, self.logcache)
end
function wal_index:read_metadata()
	return self.writer:read_state(self.store, 'metadata', self.serde, self.logcache)
end
function wal_index:write_metadata(meta)
	self.writer:write_state(self.store, 'metadata', meta, self.serde, self.logcache)
end
function wal_index:create_metadata_entry()
	self:write_metadata(self.meta)
end
function wal_index:at(idx)
	return self.logcache:at(tonumber(idx))
end
function wal_index:logs_from(start_idx)
	-- self:dump()
	return self.logcache:from(start_idx)
end
function wal_index:delete_range(start_idx, end_idx)
	local new_end_idx = self.store:delete_logs(start_idx, end_idx)
	self.logcache:delete_range(start_idx, end_idx)
	local log = self.logcache:at(new_end_idx)
	self.writer.last_index, self.writer.last_term = log.index, log.term

end
function wal_index:last_index()
	return self.writer.last_index
end
function wal_index:last_index_and_term()
	return self.writer.last_index, self.writer.last_term
end
function wal_index:dump()
	self.logcache:dump('logcache dump -- ')
end

-- module function
function _M.new(meta, store, serde, opts)
	local wal = setmetatable({
		writer = memory.alloc_fill_typed('luact_raft_wal_writer_t'),
		-- store does not fin by normal finalization for faster restart when superviser restart raft actor
		store = store,
		-- TODO : using cdata array if serde is such kind.
		logcache = ringbuf.new(opts.log_compaction_margin),
		meta = meta, 
		serde = serde,
		opts = opts,
	}, wal_mt)
	wal:init()
	return wal
end

return _M
