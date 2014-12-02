local ffi = require 'ffiex.init'
local C = ffi.C

local exception = require 'pulpo.exception'

local _M = {}

ffi.cdef [[
typedef struct luact_raft_ringbuf_header {
	uint64_t start_idx, end_idx;
	uint32_t n_size;
} luact_raft_ringbuf_header_t;
]]

-- log buffer metatable
local ringbuf_store_index = {}
local ringbuf_store_mt = {
	__index = ringbuf_store_index,
}
function ringbuf_store_index:fin()
end
function ringbuf_store_index:realloc()
end
function ringbuf_store_index:delete(i)
	self[i] = nil
end
function ringbuf_store_index:copy(spos, epos, offset)
	-- +1 for lua array index
	for i=spos+1,epos+1 do
		self[offset + i] = logs[i]
		self:delete(i)
	end
end
function ringbuf_store_index:from(spos, epos)
	-- +1 for lua array index
	if spos < epos then
		return {unpack(self, spos+1, epos+1)}
	else
		return {unpack(self, spos+1), unpack(self, 1, epos+1)}
	end
end

-- log header object
local ringbuf_header_index = {}
local ringbuf_header_mt = {
	__index = ringbuf_header_index,
}
function ringbuf_header_index:init(n_size)
	self.start_idx = 0
	self.end_idx = 0
	self.n_size = n_size
end
function ringbuf_header_index:verify_range(idx)
	return idx >= start_idx and idx <= end_idx
end
function ringbuf_header_index:at(idx, logs)
	if self:verify_range(idx) then
		local pos = idx % self.n_size
		return logs[pos]
	end
	return nil
end
function ringbuf_header_index:from(sidx)
	if self:verify_range(sidx) then
		local pos = sidx % self.n_size
		return logs:from(pos)
	end
	return nil
end
function ringbuf_header_index:reserve(size)
	local av = self:available()
	if av < size then
		local newsize = self.n_size * 2
		local required = self.n_size - av
		while required > newsize do
			newsize = newsize * 2
		end
		self = logs:realloc(newsize)
		local spos, epos = self:range_pos()
		self.n_size = newsize
		if spos > epos then
			logs:copy(0,epos,spos)
		end
	end
end
function ringbuf_header_index:append(idx, logs, log)
	if self:verify_range(idx) then
		self:reserve(1)
		local pos = idx % self.n_size
		logs[pos] = log
		self.end_idx = idx
	end
end
function ringbuf_header_index:init_at(idx, logs, init, ...)
	if self:verify_range(idx) then
		self:reserve(1)
		local pos = idx % self.n_size
		init(logs[pos], ...)
		self.end_idx = idx
	end
end
function ringbuf_header_index:delete(end_idx, logs)
	start_idx = self.start_idx
	end_idx = end_idx or self.end_idx
	if self:verify_range(end_idx) then
		for idx=start_idx,end_idx do
			local pos = index2pos(idx)
			logs:delete(pos)
		end
		self.start_idx = start_idx
	end
end
function ringbuf_header_index:index2pos(index)
	return index % self.n_size
end
function ringbuf_header_index:range_pos()
	return self:index2pos(self.start_idx), self:index2pos(end_idx)
end
function ringbuf_header_index:available()
	local spos, epos = self:range_pos()
	if spos > epos then
		return spos - epos - 1
	elseif spos < epos then
		return self.n_size - epos - 1 + spos
	elseif self.start_idx == 0 then
		return self.n_size
	else
		exception.raise('fatal', 'raft', 'invalid proposal list state', self.start_idx, self.end_idx, self.n_size)
	end
end
ffi.metatype('luact_raft_ringbuf_header_t', ringbuf_mt)


-- ringbuf object
local ringbuf_index = {}
local ringbuf_mt = {
	__index = ringbuf_index,
}
function ringbuf_index:fin()
	memory.free(self.header)
	self.store:fin()
end
function ringbuf_index:at(idx)
	self.header:at(idx, self.store)
end
function ringbuf_index:append(idx, log)
	self.header:appendt(idx, self.store, log)
end
function ringbuf_index:init_at(idx, fn, ...)
	self.header:init_at(idx, self.store, fn, ...)
end
function ringbuf_index:delete(eidx)
	self.header:delete(eidx, self.store)
end
function ringbuf_index:from(sidx)
	self.header:from(sidx)
end

-- module funcitons
function _M.new(size, store)
	local h = memory.alloc_typed('luact_raft_ringbuf_header_t')
	h:init(size)
	return setmetatable({
		header = h, 
		store = store or setmetatable({}, ringbuf_store_mt),
	}, ringbuf_mt)
end

return _M
