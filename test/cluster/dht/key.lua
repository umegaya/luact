local luact = require 'luact.init'
local key = require 'luact.cluster.dht.key'

local ok, r
local k = ffi.new('luact_dht_key_t')
k:init()
assert(k:length() == 0, "no argument initialization makes key which length == 0")
assert(key.MIN == k, "it should be same as minimum key")
assert(key.MIN <= k, "it should satisfy less than equal condition")
assert(not (key.MIN > k), "it should satisfy greater than condition")
ok, r = pcall(k.init, k, ((string.char(1)):rep(key.MAX_LENGTH + 1)))
assert(not ok and (r:is('invalid')), "if try to set byte array which length is exceed to key.MAX_LENGTH, it fails")
ok, r = pcall(k.dec, k)
assert(not ok and (r:is('invalid')), "it should not be able to decrement min key")
k:inc()
assert(k:length() == 1 and (k.p[0] == 0), "next of min key should be length = 1, byte[0] == 0")

local k2, k3, ktmp = ffi.new('luact_dht_key_t'), ffi.new('luact_dht_key_t'), ffi.new('luact_dht_key_t')
k2:init(string.char(1), 1)
k3:init(string.char(0)..(string.char(255)):rep(key.MAX_LENGTH-1))
assert(k:length() == 1, "length is same as 2nd argument of init()")
assert(k2 > k, "k2 should greater than k (MIN key)")
ktmp = k2
k2:dec()
assert(k2 == k3, "k2 should be same as k3")
k2:inc()
assert(k2 == ktmp, "k2 should be back to previous value")

local k4, k5, kbuf = ffi.new('luact_dht_key_t'), ffi.new('luact_dht_key_t'), ffi.new('luact_dht_key_t')
k4:init((string.char(255)):rep(key.MAX_LENGTH))
k5:init((string.char(255)):rep(key.MAX_LENGTH - 1)..string.char(254))
assert(k4 == key.MAX, "k4 should be same as max key")
assert(k4 > k5, "greater relation should be correct")
k5:next(kbuf)
assert(kbuf == key.MAX, "kbuf should be same as max key")
ok, r = pcall(k4.next, k4, kbuf)
assert(not ok and (r:is('invalid')))

local k6, k7 = ffi.new('luact_dht_key_t'), ffi.new('luact_dht_key_t')
k6:init((string.char(255)):rep(key.MAX_LENGTH - 1)..string.char(0))
k7:init((string.char(255)):rep(key.MAX_LENGTH - 1))
assert(k6:length() == key.MAX_LENGTH and k7:length() == (key.MAX_LENGTH - 1), 
	"with no 2nd argument, key length is same as length of given string")
assert(k6 > k7, "longer key should be greater")
k6:prev(kbuf)
assert(kbuf == k7, "when try to decrement key which last byte is 0, key length should be decrement")

local function make_key(data)
	local s = ""
	for _, ent in ipairs(data) do
		s = s .. ((string.char(ent[1])):rep(ent[2]))
	end
	local p = ffi.new('luact_dht_key_t')
	p:init(s)
	return p
end

local tests = {
	[{{0xff, 4}}] 													= {{0xff, 3}, {0xfe, 1}, {0xff, key.MAX_LENGTH - 4}},
	[{{0xff, 3}, {0x01, 1}}] 										= {{0xff, 3}, {0x00, 1}, {0xff, key.MAX_LENGTH - 4}},
	[{{0xff, 2}, {0xfe, 1}, {0xff, key.MAX_LENGTH - 3}}] 			= {{0xff, 2}, {0xfe, 1}, {0xff, key.MAX_LENGTH - 4}, {0xfe, 1}},
	[{{0xff, 2}, {0xfe, 1}, {0xff, key.MAX_LENGTH - 4}, {0x00, 1}}] = {{0xff, 2}, {0xfe, 1}, {0xff, key.MAX_LENGTH - 5}, {0xff, 1}},
	[{{0xff, 2}, {0xfe, 1}, {0xff, key.MAX_LENGTH - 4}}] 			= {{0xff, 2}, {0xfe, 1}, {0xff, key.MAX_LENGTH - 5}, {0xfe, 1}, {0xff, 1}},
	[{{0x01, 1}, {0x00, key.MAX_LENGTH - 1}}]						= {{0x01, 1}, {0x00, key.MAX_LENGTH - 2}},
	[{{0x01, 1}}]													= {{0x00, 1}, {0xff, key.MAX_LENGTH -1 }},
	[{{0x00, key.MAX_LENGTH}}]										= {{0x00, key.MAX_LENGTH - 1}},
	[{{0x00, 1}}]													= {},
}

for k,v in pairs(tests) do
	local kk, vv = make_key(k), make_key(v)
	if kk:dec() ~= vv then
		assert(false, "invalid key dec conversion:"..tostring(kk).." and "..tostring(vv))
	end
end

for k,v in pairs(tests) do
	local kk, vv = make_key(k), make_key(v)
	if kk ~= vv:inc() then
		assert(false, "invalid key inc conversion:"..tostring(kk).." and "..tostring(vv))
	end
end

return true