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
	self.last_index = 0
	self.fd = -1
end
function wal_writer_index:fin()
	self.rb:fin()
	if self.fd >= 0 then
		C.close(self.fd)
	end
end
function wal_writer_index:new_log(kind, term, index, logbody)
	return { kind = kind, term = term, index = index, log = logbody }
end
function wal_writer_index:write(store, kind, term, logs, serde, logcache, msgid)
	local last_index = self.last_index
	for i=1,#logs,1 do
		last_index = last_index + 1
		if self.last_index <= 0 then
			self.last_index = last_index
		end
		local log = self:new_log(kind, term, last_index, logs[i])
		-- last log have coroutine object to resume after these logs are accepted.
		-- (its not necessary for persisted data, so after serde:pack)
		if i == #logs and msgid then log.msgid = msgid end
		logcache:put_at(last_index, log)
		print('logat', last_index, logcache:at(last_index))
	end
	local ok, r = store:put_logs(logcache, serde, self.last_index, last_index)
	if not ok then
		exception.raise('fatal', 'raft', 'fail to commit logs', r)
	end
	self.last_index = last_index
	self.last_term = term
	return self.last_index
end
function wal_writer_index:delete(store, start_idx, end_idx)
	store:delete_logs(start_idx, end_idx)
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
	return util.table_equals(self.meta, obj)
end
function wal_index:compaction(upto_idx)
	-- remove in memory log with some margin (because minority node which hasn't replicate old log exists.)
	self.store:compaction(upto_idx - self.opts.log_compaction_margin)
	self.logcache:delete_elements(upto_idx - self.opts.log_compaction_margin)
end
function wal_index:write(kind, term, logs, msgid)
	if not pcall(self.writer.write, self.writer, self.store, kind, term, logs, self.serde, self.logcache, msgid) then
		self.store:rollback()
	end
end
function wal_index:read_state()
	return self.writer:read_state(self.store, 'state', self.serde, self.logcache)
end
function wal_index:write_state(state)
	self.writer:write_state(self.store, 'state', state, self.serde, self.logcache)
end
function wal_index:read_metadata()
	return self.writer:read_state(self.store, 'meta', self.serde, self.logcache)
end
function wal_index:write_metadata(meta)
	self.writer:write_state(self.store, 'meta', meta, self.serde, self.logcache)
end
function wal_index:at(idx)
	return self.logcache:at(idx)
end
function wal_index:logs_from(start_idx)
	return self.logcache:from(start_idx)
end
function wal_index:delete_logs(end_idx)
	self.store:delete_logs(nil, end_idx)
	self.logcache:delete_elements(end_idx)
end
function wal_index:last_index()
	return self.writer.last_index
end
function wal_index:last_index_and_term()
	return self.writer.last_index, self.writer.last_term
end
function wal_index:dump()
	print('logcache dump -- ')
	self.logcache:dump()
end

-- module function
function _M.new(meta, store, serde, opts)
	local wal = setmetatable({
		writer = memory.alloc_typed('luact_raft_wal_writer_t'),
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
