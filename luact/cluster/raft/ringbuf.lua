local ffi = require 'ffiex.init'
local C = ffi.C

local memory = require 'pulpo.memory'
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
	return self
end
function ringbuf_store_index:delete(i)
	self[i] = nil
end
function ringbuf_store_index:copy(src_store, src_pos, dst_pos)
	-- +1 for lua array index
	self[dst_pos] = src_store[src_pos]
end
function ringbuf_store_index:dump()
	print('dump ringbuf store:', self)
	for k,v in pairs(self) do
		print(k, v)
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
	if self.start_idx == 0 then
		self.start_idx = idx
		self.end_idx = idx
	end
	return idx >= self.start_idx
end
function ringbuf_header_index:at(idx, store)
	if self:verify_range(idx) then
		local pos = idx % self.n_size
		return store[pos]
	end
	return nil
end
function ringbuf_header_index:from(sidx, store)
	if self:verify_range(sidx) then
		local pos = sidx % self.n_size
		return store:from(pos)
	end
	return nil
end
function ringbuf_header_index:reserve(size, store)
	local av = self:available()
	if av < size then
		local newsize = self.n_size
		local required = size + self.n_size - av
		while required > newsize do
			newsize = newsize * 2
		end
		local newstore = store:realloc(newsize)
		for idx=tonumber(self.start_idx),tonumber(self.end_idx) do 
			local src, dst = idx % self.n_size, idx % newsize
			newstore:copy(store, src, dst)
		end
		self.n_size = newsize
		return newstore
	end
	return store
end
function ringbuf_header_index:put_at(idx, store, log)
	if self:verify_range(idx) then
		local diff = idx - self.end_idx
		if diff > 0 then
			store = self:reserve(diff, store)
		end
		store[idx % self.n_size] = log
		if diff > 0 then
			self.end_idx = idx
		end
	end
	return store
end
function ringbuf_header_index:init_at(idx, store, init, ...)
	if self:verify_range(idx) then
		local diff = idx - self.end_idx
		if diff > 0 then
			store = self:reserve(diff, store)
		end
		init(store[idx % self.n_size], ...)
		if diff > 0 then
			self.end_idx = idx
		end
	end
	return store
end
function ringbuf_header_index:delete(end_idx, store)
	if self:verify_range(end_idx) then
		local start_idx = tonumber(self.start_idx)
		end_idx = end_idx or self.end_idx
		for idx=start_idx,end_idx do
			local pos = idx % self.n_size
			store:delete(pos)
		end
		self.start_idx = end_idx + 1
		if self.end_idx <= end_idx then
			self.end_idx = end_idx + 1
		end
	end
end
function ringbuf_header_index:index2pos(index)
	return index % self.n_size
end
function ringbuf_header_index:range_pos()
	return self:index2pos(self.start_idx), self:index2pos(self.end_idx)
end
function ringbuf_header_index:available()
	local spos, epos = self:range_pos()
	if spos > epos then
		return spos - epos - 1
	elseif spos < epos then
		return self.n_size - epos - 1 + spos
	elseif spos == epos then
		return self.n_size
	else
		exception.raise('fatal', 'raft', 'invalid ring buffer state', self.start_idx, self.end_idx, self.n_size)
	end
end
function ringbuf_header_index:dump()
	print('header:', self.start_idx, self.end_idx, self.n_size, self:available())
end
ffi.metatype('luact_raft_ringbuf_header_t', ringbuf_header_mt)


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
	return self.header:at(idx, self.store)
end
function ringbuf_index:put_at(idx, log)
	self.store = self.header:put_at(idx, self.store, log)
end
function ringbuf_index:init_at(idx, fn, ...)
	self.store = self.header:init_at(idx, self.store, fn, ...)
end
function ringbuf_index:delete(eidx)
	self.header:delete(eidx, self.store)
end
function ringbuf_index:from(sidx)
	return self.header:from(sidx)
end
function ringbuf_index:available()
	return self.header:available()
end
function ringbuf_index:dump()
	self.header:dump()
	self.store:dump()
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
