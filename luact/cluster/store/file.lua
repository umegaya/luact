local ffi = require 'ffiex.init'
local C = ffi.C

local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local pbuf = require 'pulpo.pbuf'
local fs = require 'pulpo.fs'

local _M = {}

-- cdefs
ffi.cdef [[
typedef struct luact_store_file {
	const char *dir;
	luact_rbuf_t rb;
	int fd; //for wal
} luact_store_file_t;
]]

-- luact_store_file
local store_file_index = {}
local store_file_mt = {
	__index = store_file_index
}
function store_file_index:init(dir, name, opts)
	self.dir = memory.strdup(dir)
	self.rb:init()
	self:open()
end
function store_file_index:path()
	return util.rawsprintf(fs.path("%s", "%16x.wal"), self.dir, self.last_index + 1)
end
function store_file_index:state_path()
	return util.rawsprintf(fs.path("%s", "state"), self.dir)
end
function store_file_index:replica_set_path()
	return util.rawsprintf(fs.path("%s", "replicas"), self.dir)
end
function store_file_index:path_by_kind(kind)
	if kind == 'state' then
		return self:state_path()
	elseif kind == 'replica_set' then
		return self:replica_set_path()
	else
		exception.raise('invalid', 'objects type', kind)
	end
end
function store_file_index:open()
	local path = self:path()
	self.fd = C.open(path, bit.bor(fs.O_APPEND, fs.O_WRONLY))
	if self.fd < 0 then
		exception.raise('fatal', 'cannot open wal file', ffi.string(path), ffi.errno())
	end
end
-- remove/search snapshot files
local wal_pattern = fs.path(".*", "([0-9a-f]+)%.wal$")
function store_file_index:remove_files_by_index(dir, index)
	local files = {}
	local d = fs.opendir(dir)
	for path in d:iter() do
		if path:match(wal_pattern) then
			table.insert(files, path)
		end
	end
	-- default sort is ascending order
	table.sort(files)
	for i=1,#files,1 do
		local ok, idx = files[i]:match(wal_pattern)
		if ok then
			idx = tonumber(idx, 16)
			if idx <= index then break end
		end
	end
	for j=i+1,#files,1 do
		logger.warn('wal file removed:', files[i])
		C.unlink(files[i])
	end
	return nil
end
function store_file_index:covered_files_for_index(dir, index)
	local files = {}
	local d = fs.opendir(dir)
	for path in d:iter() do
		if path:match(wal_pattern) then
			table.insert(files, path)
		end
	end
	-- default sort is ascending order
	table.sort(files)
	for i=#files,1,-1 do
		local ok, idx = files[i]:match(wal_pattern)
		if ok then
			idx = tonumber(idx, 16)
			if idx <= index then
				-- array of files[idx], ..., files[#files]
				return {unpack(files, i)}
			end
		end
	end
	return nil
end
function store_file_index:flush_rbuf(fd)
	while self.rb:available() > 0 do
		self.rb:use(C.write(fd or self.fd, self.rb:curr_p(), self.rb:available()))
	end
end

-- interfaces for consensus modules
function store_file_index:compact(upto_idx)
	if self.fd >= 0 then C.close(self.fd) end
	self:open()
	-- remove files which does not contains logs which index > upto_idx
	self:remove_files_by_index(dir, upto_idx)
end
function store_file_index:put_object(kind, serde, object)
	local path = self:path_by_kind(kind)
	local fd = C.open(path, bit.bor(fs.O_TRUNC, fs.O_WRONLY))
	self.rb:reset()
	serde:pack(self.rb, object)
	self:flush_rbuf(fd)
	C.fsync(fd)
	C.close(fd)
end
function store_file_index:get_object(kind, serde)
	local path = self:path_by_kind(kind)
	self.rb:reset()
	fs.load2rbuf(path, self.rb)
	local obj, err = serde:unpack(rb)
	if obj then
		exception.raise('fatal', 'invalid format for serde', err)
	end
	return obj
end
function store_file_index:put_logs(logcache, serde, start_idx, end_idx)
	self.rb:reset()
	for idx=start_idx,end_idx do
		local log = logcache:at(idx)
		serde:pack(self.rb, log)
	end
	self:flush_rbuf()
	C.fsync(self.fd)
	return true
end
-- TODO : following interfaces requires to preserve idx => offset table. (with transactional way)
function store_file_index:delete_logs(start_idx, end_idx)
end
function store_file_index:get_log(idx, serde)
end



-- module functions
function _M.new(dir, name, opts)
	local p = memory.alloc_typed('luact_store_file_t')
	p:init(dir, name, opts)
	return p
end

