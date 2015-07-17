local luact = require 'luact.init'
local lamport = require 'pulpo.lamport'
local cache = require 'luact.cluster.dht.cache'

local function dummy_range(start_key, end_key)
	return {
		start_key = start_key,
		end_key = end_key,
		include = function (self, k, kl)
			return self.start_key:less_than(k, kl) and (not self.end_key:less_than(k, kl))
		end,
	}
end

local c = cache.new_range(1)
assert(c.kind == 1, "kind should be ctor argument")


local range_start_keys = {}

for i=("a"):byte(),("z"):byte() do
	local ks, ke = ffi.new('luact_dht_key_t'), ffi.new('luact_dht_key_t')
	ks:init(string.char(i), 1)
	ke:init(string.char(i+1), 1):dec()
	table.insert(range_start_keys, ks)
	c:add(dummy_range(ks, ke))
end

local batch = {}
for i=("Z"):byte(),("A"):byte(),-1 do
	local ks, ke = ffi.new('luact_dht_key_t'), ffi.new('luact_dht_key_t')
	ks:init(string.char(i), 1)
	ke:init(string.char(i+1), 1):dec()
	table.insert(range_start_keys, ks)
	table.insert(batch, dummy_range(ks, ke))
end
c:batch_add(batch)
table.sort(range_start_keys, function (a, b)
	return a < b
end)
assert(#c == 52, "all range data should correctly be added")

for i=1,#c do
	assert(c[i].start_key == range_start_keys[i], "cached key should be sorted")
end

for i=("a"):byte(),("z"):byte() do
	local k = (string.char(i)):rep(512)
	local r = c:find(k)
	-- print(r, r.start_key.p[0], i, ffi.string(r.start_key.p, r.start_key.length))
	assert(r and (r.start_key.p[0] == i))
end

for i=("a"):byte(),("z"):byte() do
	local k = (string.char(i)):rep(512)
	local r = c:find(k)
	c:remove(r)
	-- print(r, r.start_key.p[0], i, ffi.string(r.start_key.p, r.start_key:length()))
	assert(not c:find(k))
end

local dummy_rm = {
	clock = {
		issue = function (self)
			return self.value
		end,
	},
	skew = 0.25,
	duration = 10,
	max_clock_skew = function (self)
		return self.skew
	end,
	ts_cache_duration = function (self)
		return self.duration 
	end,
}

local init_ts, low_water = lamport.debug_make_hlc(1, 10000), lamport.debug_make_hlc(1, 10000)
dummy_rm.clock.value = init_ts
local c2 = cache.new_ts(dummy_rm)
local a, b, c, d, e, f = "A", "B", "C", "D", "E"
local nc = "C\0"
local rts, wts
local ts1, ts2, ts3 = lamport.debug_make_hlc(1, 9999), lamport.debug_make_hlc(1, 10001), lamport.debug_make_hlc(1, 10002)
low_water:add_walltime(dummy_rm.skew)
ts1:add_walltime(dummy_rm.skew)
ts2:add_walltime(dummy_rm.skew)
ts3:add_walltime(dummy_rm.skew)
-- add ts1 for A - C (read) it evicted immediately
-- rts and wts is same as low_water
c2:add(a, #a, c, #c, ts1, true)
assert(#c2 == 0)
rts, wts = c2:latest_ts(a, #a, c, #c)
assert(rts == low_water and wts == low_water)
rts, wts = c2:latest_ts(c, #c, e, #e)
assert(rts == low_water and wts == low_water)
-- add ts3 for A - C (read)
-- wts is low_water and rts is ts3 (for A - C), low_water (for C - E)
c2:add(a, #a, c, #c, ts3, true)
assert(#c2 == 1)
rts, wts = c2:latest_ts(a, #a, c, #c)
assert(rts == ts3 and wts == low_water)
rts, wts = c2:latest_ts(c, #c, e, #e)
assert(rts == low_water and wts == low_water)
-- add ts2 for B - C (read), because ts2 < ts3 and A - C contains B - C, 
-- nothing changes
c2:add(b, #b, c, #c, ts2, true)
assert(#c2 == 1)
-- add ts2 for B - D (read), because ts2 < ts3 but A - C does not contains B - D, 
-- rts is ts3 (for A - C), ts2 (for C - D), low_water (D - E)
c2:add(b, #b, d, #d, ts2, true)
assert(#c2 == 2)
rts, wts = c2:latest_ts(a, #a, c, #c)
assert(rts == ts3 and wts == low_water)
rts, wts = c2:latest_ts(c, #c, d, #d)
assert(rts == ts2 and wts == low_water)
rts, wts = c2:latest_ts(d, #d, e, #e)
assert(rts == low_water and wts == low_water)
-- add ts3 for D - E (read), 
-- rts is ts3 (for A - C), ts2 (for C, C\0), ts3 (D - E)
c2:add(d, #d, e, #e, ts3, true)
assert(#c2 == 3)
rts, wts = c2:latest_ts(a, #a, c, #c)
assert(rts == ts3 and wts == low_water)
rts, wts = c2:latest_ts(c, #c, nc, #nc)
assert(rts == ts2 and wts == low_water)
rts, wts = c2:latest_ts(d, #d, e, #e)
assert(rts == ts3 and wts == low_water)
-- add ts2 for A - C (write),
-- rts is unchanged from above and wts is ts2 (A - C), low_water (C - E)
c2:add(a, #a, c, #c, ts2, false)
assert(#c2 == 4)
rts, wts = c2:latest_ts(a, #a, c, #c)
assert(rts == ts3 and wts == ts2)
rts, wts = c2:latest_ts(c, #c, nc, #nc)
assert(rts == ts2 and wts == low_water)
rts, wts = c2:latest_ts(d, #d, e, #e)
assert(rts == ts3 and wts == low_water)

return true







