local ffi = require 'ffiex.init'
local C = ffi.C

local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local pbuf = require 'pulpo.pbuf'
local fs = require 'pulpo.fs'

local sha2 = require 'lua-aws.deps.sha2'

ffi.cdef [[
typedef struct luact_raft_snapshot_header {
	uint8_t checksum[32]; //sha2 hash
	uint64_t size;
	uint64_t term;
	uint64_t index;
	uint16_t n_replica, replica_transition;
	luact_uuid_t replicas[0];
} luact_raft_snapshot_header_t;
typedef struct luact_raft_snapshot_writer {
	luact_rbuf_t rb, rbfsm; 		// workmem for packing log data
	uint64_t last_snapshot_idx;	// last log entry index in wal
	uint64_t last_snapshot_term;
} luact_raft_snapshot_writer_t;
]]

-- luact_raft_snapshot_header
local snapshot_header_index = {}
local snapshot_header_mt = {
	__index = snapshot_header_index
}
function snapshot_header_size(n_replica)
	return ffi.sizeof('luact_raft_snapshot_header_t') + (n_replica * ffi.sizeof('luact_uuid_t')))
end
function snapshot_header_index.alloc(n_replica)
	local p = memory.alloc_fill_typed(snapshot_header_size(n_replica))
	p.n_replica = n_replica
	return ffi.cast('luact_raft_snapshot_header_t*', p)
end
function snapshot_header_index:realloc(n_replica)
	local p = memory.realloc(self, snapshot_header_size(n_replica))
	p.n_replica = n_replica
	return ffi.cast('luact_raft_snapshot_header_t*', p)
end
function snapshot_header_index:to_table()
	local t = {}
	for i=1,self.n_replica do
		table.insert(t, self.replicas[i - 1])
	end
	return t
end
function snapshot_header_index.pack(tbl, arg)
	return ffi.string(arg, snapshot_header_size(arg.n_replica))
end
function snapshot_header_index.unpack(arg)
	return ffi.cast('luact_raft_snapshot_header_t*', arg)
end
ffi.metatype('luact_raft_snapshot_header_t', snapshot_header_mt)


-- luact_raft_snapshot_writer
local snapshot_writer_index = {}
local snapshot_writer_mt = {
	__index = snapshot_writer_index
}
function snapshot_writer_index:init()
	self.rb:init()
	self.rbfsm:init()
end
function snapshot_writer_index:fin()
	self.rb:fin()
	self.rbfsm:fin()
end
function snapshot_writer_index:path(dir, last_applied_idx)
	return util.rawsprintf(fs.path("%s", "%16x.snap"), dir, last_applied_idx)
end
function snapshot_writer_index:open(dir, last_applied_idx)
	local path = self:path(dir, last_applied_idx)
	local fd = C.open(path, bit.bor(fs.O_CREAT, fs.O_WRONLY))
	if fd < 0 then
		exception.raise('fatal', 'cannot open snapshot file', ffi.string(path), ffi.errno())
	end
	return fd
end
local snapshot_header = snapshot_header_index.alloc(3)
function snapshot_writer_index:write(dir, fsm, st, serde)
	self.rbfsm:reset()
	local ok, r = pcall(fsm.snapshot, fsm, serde, self.rbfsm)
	if not ok then
		logger.error('fail to take snapshot:', r)
		return
	end
	self.rb:reset()
	-- put metadata at first of snapshot file
	if snapshot_header.n_replica < #st.replicas then
		snapshot_header = snapshot_header:realloc(#st.replicas)
	end
	snapshot_header.term = st:current_term()
	snapshot_header.size = self.rbfsm:available()
	local checksum = sha2.hash256(self.rbfsm:start_p(), self.rbfsm:available())
	ffi.copy(snapshot_header.checksum, checksum, 32)
	snapshot_header.index = st.state.last_applied_idx
	snapshot_header.n_replica = #st.replicas
	snapshot_header.replica_transition = st.replica_transition and 1 or 0
	for i=1,#st.replicas do
		snapshot_header[i - 1] = st.replicas[i]
	end
	serde:pack(self.rb, snapshot_header)
	local fd = self:open(dir, last_applied_idx)
	-- TODO : use pulpo.io and wait io when all bytes are written
	C.write(fd, self.rb:start_p(), self.rb:available())
	C.write(fd, self.rbfsm:start_p(), self.rbfsm:available())
	C.fsync(fd)
	C.close(fd)
	self.last_snapshot_idx = last_applied_idx
	self.last_snapshot_term = term
	return self.last_snapshot_idx
end
function snapshot_writer_index:latest_snapshot_path()
	local d = fs.opendir(self.path)
	local latest 
	for _, path in d:iter() do
		if path:match(fs.path(".*", "[0-9a-f]+%.snap$")) then
			if (not latest) or latest < path then
				latest = path
			end
		end
	end
	return latest
end
function snapshot_writer_index:restore(dir, fsm, serde)
	local latest = self:latest_snapshot_path()
	if not latest then
		return
	end
	local rb = fs.load2rbuf(latest)
	if not rb then
		-- TODO : can continue with older snapshot (or should not)?
		exception.raise('fatal', 'cannot open snapshot file', latest)
	end
	local meta, err = serde:unpack(rb)
	if (not meta) or (not meta.index) then
		rb:fin()
		-- TODO : can continue with older snapshot (or should not)?
		exception.raise('fatal', 'invalid snapshot file', latest, err)
	end
	local ok, r = pcall(fsm.restore, fsm, serde, rb)
	rb:fin()
	if not ok then
		exception.raise('fatal', 'cannot restore from snapshot', latest, r)
	end
	self.last_snapshot_idx = meta.index
	return self.last_snapshot_idx
end
ffi.metatype('luact_raft_snapshot_writer_t', snapshot_writer_mt)

-- snapshot object
local snapshot_index = {}
local snapshot_mt = {
	__index = snapshot_index
}
function snapshot_index:init()
	self.writer:init()
end
function snapshot_index:fin()
	self.writer:fin()
end
function snapshot_index:write(fsm, state)
	return self.writer:write(self.dir, fsm, state, self.serde)
end
function snapshot_index:restore(fsm)
	return self.writer:restore(self.dir, fsm, self.serde)
end
function snapshot_index:last_index_and_term()
	return self.writer.last_snapshot_term, self.writer.last_snapshot_idx
end
function snapshot_index:latest_snapshot()
	local path = self.writer:latest_snapshot_path()
	local rb = (path and fs.load2rbuf(path))
	if rb then 
		return rb, self.serde, rb:available() 
	end
	return nil
end

-- module functions
_M.serde_initialized = false
function _M.new(dir, serde)
	local ss = setmetatable({
		writer = memory.alloc_typed('luact_raft_snapshot_writer_t'),
		serde = serde,
		dir = dir,
	}, snapshot_mt)
	ss:init()
	if not _M.serde_initialized then
		serde:customize('struct luact_raft_snapshot_header', snapshot_header_index.pack, snapshot_header_index.unpack)
	end
	return ss
end

return _M
