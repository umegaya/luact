local ffi = require 'ffiex.init'
local memory = require 'pulpo.memory'
local key = require 'luact.cluster.dht.key'
local uuid = require 'luact.uuid'

local _M = {}

ffi.cdef [[
typedef struct luact_dht_ts_cache_entry {
	luact_dht_key_range_t range;
	luact_uuid_t txn_id;
	pulpo_hlc_t ts;
	bool read;
} luact_dht_ts_cache_entry_t;
]]

-- cache
-- TODO : implement red black tree with FFI for faster insert/lookup
-- TODO : cockroach make cache size limitation. implement it
local range_cache_mt = {}
range_cache_mt.__index = range_cache_mt
function range_cache_mt:find(k, kl)
	if not k then
		logger.error('invalid key:', k)
	end
	kl = kl or #k
	for i=1,#self do
		-- logger.info('cache search', self[i].start_key, self[i]:include(k, kl))
		if self[i]:include(k, kl) then
			return self[i]
		end
	end
	return nil
end
function range_cache_mt:sort()
	table.sort(self, function (a, b)
		return a.end_key < b.end_key
	end)
end
function range_cache_mt:add(r)
	local tmp = self:find(r.end_key:as_slice())
	if not tmp then
		table.insert(self, r)
		self:sort()
		return r
	end
	return tmp
end
function range_cache_mt:remove(r)
	local k, kl = r.end_key:as_slice()
	for i=1,#self do
		-- logger.info(self[i].start_key, ("%02x"):format(k:byte()), 
		-- 	self[i].end_key:less_than_equals(k, kl), self[i].start_key:less_than_equals(k, kl))
		if self[i]:include(k, kl) then
			table.remove(self, i)
			break
		end
	end
end
function range_cache_mt:batch_add(ranges)
	for i=1,#ranges do
		table.insert(self, ranges[i])
	end
	self:sort()
end
function range_cache_mt:each_belongs_to_self(block, ...)
	self:each_belongs_to(nil, block, ...)
end
function range_cache_mt:each_belongs_to(node, block, ...)
	for i=1,#self do
		if self[i]:belongs_to(node) then
			block(self[i], ...)
		end
	end
end
function range_cache_mt:clear(dtor)
	for i=#self,1,-1 do
		local c = self[i]
		table.remove(self, i)
		if dtor then
			dtor(c)
		end
	end
end


local ts_cache_ent_mt = {}
ts_cache_ent_mt.__index = ts_cache_ent_mt
function ts_cache_ent_mt:earlier(ent)
end

local ts_cache_mt = {}
ts_cache_mt.__index = ts_cache_mt
function ts_cache_mt:create_ent(k, kl, ek, ekl, ts, txn, read)
	local ent = memory.alloc_typed('luact_dht_ts_cache_entry_t')
	ent.range:init(k, kl, ek, ekl)
	ent.ts = ts
	ent.read = read
	if txn then
		ent.txn_id = txn:uuid()
	else
		uuid.invalidate(ent.txn_id)
	end
	return ent
end
function ts_cache_mt:init()
	local ts = self.rm.clock:issue()
	ts:add_walltime(self.rm:max_clock_skew())
	ts:copy_to(self.low_water)
	ts:copy_to(self.latest)
end
function ts_cache_mt:fin()
	self:clear()
	memory.free(self.latest)
	memory.free(self.low_water)
	memory.free(self.work)
end
function ts_cache_mt:clear()
	self:init()
	for i=1,#self do
		memory.free(self[i])
		self[i] = nil
	end
end
function ts_cache_mt:add(k, kl, ek, ekl, ts, txn, read)
	if ts < self.low_water then
		return 
	end
	if ts > self.latest then
		ts:copy_to(self.latest)
	end
	local ent = self:create_ent(k, kl, ek, ekl, ts, txn, read)
	self:add_ent(ent)
end
function ts_cache_mt:add_ent(ent)
	for i=#self,1,-1 do
		local e = self[i]
		if e.read == ent.read then
			if ent.range:contains_range(e.range) and ent.ts >= e.ts then
				table.remove(self, i)
				memory.free(e)
			elseif e.range:contains_range(ent.range) and e.ts >= ent.ts then
				memory.free(ent)
				return
			end
		end
	end
	-- logger.error('added', ent.range, ent.ts)
	table.insert(self, ent)
	self:evict()
	return true
end
function ts_cache_mt:merge(tsc)
	for i=1,#tsc do
		self:add_ent(tsc[i])
	end
end
function ts_cache_mt:evict()
	self.latest:copy_to(self.work)
	self.work:add_walltime(-1 * self.rm:ts_cache_duration())
	for i=#self,1,-1 do
		local ent = self[i]
		local ts = ent.ts
		if ts < self.low_water then
			table.remove(self, i)
			memory.free(ent)
		elseif ts < self.work then
			ts:copy_to(self.low_water)
			table.remove(self, i)
			memory.free(ent)
		end
	end
end
function ts_cache_mt:each_intersection_of(k, kl, ek, ekl, proc, ...)
	for i=#self,1,-1 do
		if self[i].range:intersects_key_slice_range(k, kl, ek, ekl) then
			local r = proc(self[i], ...)
			if r then return r end
		end	
	end
end
-- returns latest read ts and write ts. 
-- caution : these retvals are volatile. you need to copy them somewhere when you use it after.
function ts_cache_mt:latest_ts(k, kl, ek, ekl, txn, dump)
	if not ek then
		ek = ffi.string(k, kl)..string.char(0)
		ekl = #ek
	end
	local txn_id = txn and txn:uuid()
	local ret = { self.low_water, self.low_water }
	self:each_intersection_of(k, kl, ek, ekl, function (ent, ret)
		if dump then
			logger.info(ent.range, ent.read, ent.ts, ent.txn_id)
		end
		if (not (txn_id and uuid.valid(txn_id))) or (not uuid.valid(ent.txn_id)) or (not uuid.equals(txn_id, ent.txn_id)) then
			if ent.read and ((not ret[1]) or (ret[1] < ent.ts)) then
				ret[1] = ent.ts
			elseif (not ent.read) and ((not ret[2]) or (ret[2] < ent.ts)) then
				ret[2] = ent.ts
			end
		end
	end, ret)
	-- logger.report('latest_ts', ffi.string(k, kl), ek and ffi.string(ek, ekl) or "[empty]", self.low_water, unpack(ret))
	return unpack(ret)
end

-- module function 
function _M.new_range(kind)
	return setmetatable({kind = kind}, range_cache_mt)
end
function _M.new_ts(rm)
	local low_water, latest = memory.alloc_typed('pulpo_hlc_t'), memory.alloc_typed('pulpo_hlc_t')
	local c = setmetatable({ rm = rm, low_water = low_water, latest = latest, work = memory.alloc_typed('pulpo_hlc_t') }, ts_cache_mt)
	c:init()
	return c
end

return _M
