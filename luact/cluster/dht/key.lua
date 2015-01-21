local ffi = require 'ffiex.init'
local memory = require 'pulpo.memory'


-- module table
local _M = {}


-- constant
_M.PARTITION_LENGTH = 254
_M.MIN = memory.alloc_fill_typed('luact_dht_key_t')
_M.MAX = memory.alloc_fill_typed('luact_dht_key_t', 1, _M.PARTITION_KEY_LENGTH)


-- cdefs
ffi.cdef(([[
typedef struct luact_dht_key {
	uint16_t length;
	union {
		uint8_t data[%d];
		uint8_t p[0];
	};
} luact_dht_key_t;
]]):format(_M.PARTITION_LENGTH))


-- key
local key_mt = {}
key_mt.__index = key_mt
function key_mt:init(k, kl)
	self.length = kl
	ffi.copy(self.data, k, kl)
end
function key_mt:prev()
end
function key_mt:next()
end
function key_mt:equals(k, kl)
	if self.length ~= kl then
		return false
	else
		return memory.cmp(self.p, k, self.length)
	end
end
function key_mt:less_than(k, kl)
	if self.length < kl then
		return memory.rawcmp(self.p, k, self.length) <= 0 
	elseif self.length >= kl then
		return memory.rawcmp(self.p, k, kl) < 0
	end
end
function key_mt:less_than_equals(k, kl)
	if self.length < kl then
		return memory.rawcmp(self.p, k, self.length) <= 0 
	elseif self.length > kl then
		return memory.rawcmp(self.p, k, kl) < 0
	else
		return memory.rawcmp(self.p, key.p, self.length) <= 0 
	end
end
function key_mt:__eq(key)
	return self:equals(key.p, key.length)
end
function key_mt:__lt(key)
	return self:less_than(key.p, key.length)
end
function key_mt:__le(key)
	return self:less_than_equals(key.p, key.length)
end
ffi.metatype('luact_dht_key_t', key_mt)

return _M
