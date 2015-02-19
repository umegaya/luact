local luact = require 'luact.init'
local cache = require 'luact.cluster.dht.cache'

local function dummy_range(start_key, end_key)
	return {
		start_key = start_key,
		end_key = end_key,
	}
end

local c = cache.new(1)
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
	-- print(r, r.start_key.p[0], i, ffi.string(r.start_key.p, r.start_key.length))
	assert(not c:find(k))
end

return true