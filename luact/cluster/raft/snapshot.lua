local ffi = require 'ffiex.init'
local C = ffi.C

local pbuf = require 'luact.pbuf'
local serde = require 'luact.serde'

local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local fs = require 'pulpo.fs'

local sha2 = require 'lua-aws.deps.sha2'

local _M = {}


ffi.cdef [[
// TODO : add encoding type (serpent, json, msgpack/compressed, plain)
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
	return ffi.sizeof('luact_raft_snapshot_header_t') + (n_replica * ffi.sizeof('luact_uuid_t'))
end
function snapshot_header_index.alloc(n_replica)
	local p = memory.alloc_fill(snapshot_header_size(n_replica))
	p = ffi.cast('luact_raft_snapshot_header_t*', p)
	p.n_replica = n_replica
	return p
end
function snapshot_header_index:realloc(n_replica)
	local p = memory.realloc(self, snapshot_header_size(n_replica))
	p = ffi.cast('luact_raft_snapshot_header_t*', p)
	p.n_replica = n_replica
	return p
end
function snapshot_header_index:to_table()
	local t = {}
	for i=1,self.n_replica do
		table.insert(t, self.replicas[i - 1])
	end
	return t
end
function snapshot_header_index:verify_checksum(p, size)
	if self.size ~= size then
		return false
	end
	local checksum = sha2.hash256(ffi.string(p, size))
	for i=1,32 do
		if checksum:byte(i) ~= self.checksum[i - 1] then
			return false
		end
	end
	return true
end
function snapshot_header_index.pack(arg)
	--for i=1,arg.n_replica do
	--	print('id', arg.replicas[i - 1])
	--end
	--print('len', arg.n_replica, snapshot_header_size(arg.n_replica))
	return ffi.string(arg, snapshot_header_size(arg.n_replica))
end
function snapshot_header_index.unpack(arg)
	return ffi.cast('luact_raft_snapshot_header_t*', arg)
end
serde[serde.kind.serpent]:customize(
	'struct luact_raft_snapshot_header', 
	snapshot_header_index.pack, snapshot_header_index.unpack
)
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
function snapshot_writer_index:snapshot_path(dir, last_applied_idx)
	return util.rawsprintf(fs.path("%s", "%08x.snap"), 
		#dir + 16 + #(".snap") + 1, dir, 
		ffi.new('uint64_t', last_applied_idx)
	)
end
function snapshot_writer_index:open(dir, last_applied_idx)
	local path = self:snapshot_path(dir, last_applied_idx)
	local fd = C.open(path, bit.bor(fs.O_CREAT, fs.O_EXCL, fs.O_WRONLY), fs.mode("640"))
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
	snapshot_header = st:write_snapshot_header(snapshot_header)
	snapshot_header.size = self.rbfsm:available()
	local checksum = sha2.hash256(ffi.string(self.rbfsm:start_p(), self.rbfsm:available()))
	ffi.copy(snapshot_header.checksum, checksum, 32)
	serde:pack(self.rb, snapshot_header)

	local fd = self:open(dir, snapshot_header.index)
	-- TODO : use pulpo.io and wait io when all bytes are written
	C.write(fd, self.rb:start_p(), self.rb:available())
	C.write(fd, self.rbfsm:start_p(), self.rbfsm:available())
	C.fsync(fd)
	C.close(fd)
	self.last_snapshot_idx = snapshot_header.index
	self.last_snapshot_term = snapshot_header.term
	--print('wr',self.last_snapshot_idx)
	return self.last_snapshot_idx
end
function snapshot_writer_index:latest_snapshot_path(dir)
	local d = fs.opendir(dir)
	local latest 
	for path in d:iter() do
		if path:match(_M.path_pattern) then
			if (not latest) or (latest < path) then
				latest = path
			end
		end
	end
	return latest and fs.path(dir, latest)
end
function snapshot_writer_index:remove_oldest_snapshot(dir, margin)
	local d = fs.opendir(dir)
	local oldest
	local count = 0
	for path in d:iter() do
		if path:match(_M.path_pattern) then
			count = count + 1
			if (not oldest) or (oldest > path) then
				oldest = path
			end
		end
	end
	if margin < count then
		fs.rm(fs.path(dir, oldest))
	end
end
function snapshot_writer_index:restore(dir, fsm, serde)
	local latest = self:latest_snapshot_path(dir)
	if not latest then
		logger.notice('will not restore: no snapshot under', dir)
		return
	end
	logger.notice('restore with', latest)
	local rb = fs.load2rbuf(latest)
	if not rb then
		-- TODO : can continue with older snapshot (or should not)?
		exception.raise('fatal', 'cannot open snapshot file', latest, ffi.errno())
	end
	local meta, err = serde:unpack(rb)
	if (not meta) or (not meta.index) then
		rb:fin()
		-- TODO : can continue with older snapshot (or should not)?
		exception.raise('fatal', 'invalid snapshot file', latest, err)
	end
	if not meta:verify_checksum(rb:curr_p(), rb:available()) then
		rb:fin()
		exception.raise('fatal', 'invalid snapshot checksum')
	end
	local ok, r = pcall(fsm.restore, fsm, serde, rb)
	rb:fin()
	if not ok then
		exception.raise('fatal', 'cannot restore from snapshot', latest, r)
	end
	self.last_snapshot_idx = meta.index
	self.last_snapshot_term = meta.term
	return self.last_snapshot_idx, meta
end
ffi.metatype('luact_raft_snapshot_writer_t', snapshot_writer_mt)

-- snapshot object
local snapshot_index = {}
local snapshot_mt = {
	__index = snapshot_index
}
function snapshot_index:init()
	self.writer:init()
	fs.mkdir(self.dir)
end
function snapshot_index:fin()
	self.writer:fin()
	memory.free(self.writer)
end
function snapshot_index:write(fsm, state)
	return self.writer:write(self.dir, fsm, state, self.serde)
end
function snapshot_index:trim(margin)
	self.writer:remove_oldest_snapshot(self.dir, margin)
end
function snapshot_index:restore(fsm)
	return self.writer:restore(self.dir, fsm, self.serde)
end
function snapshot_index:last_index()
	return self.writer.last_snapshot_idx
end
function snapshot_index:last_index_and_term()
	return self.writer.last_snapshot_idx, self.writer.last_snapshot_term
end
function snapshot_index:path_of(idx)
	return self.writer:snapshot_path(self.dir, idx)
end
function snapshot_index:latest_snapshot()
	local path = self.writer:latest_snapshot_path(self.dir)
	local rb = (path and fs.load2rbuf(path))
	if rb then 
		return rb, self.serde, rb:available() 
	end
	return nil
end

-- module functions
_M.serde_initialized = false
_M.path_pattern = "[0-9a-f]+%.snap$"
function _M.new(dir, sr)
	local ss = setmetatable({
		writer = memory.alloc_fill_typed('luact_raft_snapshot_writer_t'),
		serde = sr,
		dir = dir,
	}, snapshot_mt)
	ss:init()
	return ss
end

return _M
