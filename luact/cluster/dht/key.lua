local ffi = require 'ffiex.init'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local buffer = require 'luact.util.buffer'


-- module table
local _M = {}


-- constant
_M.MAX_LENGTH = 255


-- cdefs
ffi.cdef(([[
typedef struct luact_dht_key {
	uint8_t len;
	union {
		uint8_t data[%d];
		uint8_t p[0];
	};
} luact_dht_key_t;
typedef struct luact_dht_key_range {
	luact_dht_key_t s, e;
} luact_dht_key_range_t;
]]):format(_M.MAX_LENGTH))


-- constant cdata
_M.MIN = (memory.alloc_fill_typed('luact_dht_key_t'))[0]
_M.MIN.len = 0
_M.MAX = (memory.alloc_fill_typed('luact_dht_key_t', 0xFF, _M.MAX_LENGTH))[0]
_M.MAX.len = _M.MAX_LENGTH


-- key
local key_mt = buffer.new_mt()
function key_mt:init(k, kl)
	kl = kl or (k and #k or 0)
	if kl > _M.MAX_LENGTH then
		exception.raise('invalid', 'too long key', kl)
	end
	self.len = kl
	if kl > 0 then
		ffi.copy(self.data, k, kl)
	end
	return self
end
function key_mt:copy_to(buf)
	ffi.copy(buf, self, self.len + 1)
end
function key_mt:value_ptr()
	return self.p
end
function key_mt:length()
	return self.len
end
function key_mt:set_length(len)
	self.len = len
end
function key_mt:max_length()
	return _M.MAX_LENGTH
end
function key_mt:make_greater_than_prefix_keys()
	for i=tonumber(self.len)-1,0,-1 do
		self.p[i] = self.p[i] + 1
		if self.p[i] ~= 0 then
			break
		end
	end
end

ffi.metatype('luact_dht_key_t', key_mt)

local key_range_mt = {}
key_range_mt.__index = key_range_mt
function key_range_mt:init(sk, skl, ek, ekl)
	self.s:init(sk, skl)
	if ek then
		self.e:init(ek, ekl)
	else
		self.s:next(self.e)
	end
	if self.e <= self.s then
		assert(false, "invalid range:"..tostring(self))
	end
end
function key_range_mt:contains(k)
	return self.s <= k and k < self.e
end
function key_range_mt:contains_range(kr)
	return self:contains(kr.s) and (kr.e <= self.e)
end
function key_range_mt:contains_key_slice(k, kl)
	-- s <= (k, kl) < e
	return (not self.e:less_than_equals(k, kl)) and self.s:less_than_equals(k, kl)
end
function key_range_mt:intersects_key_slice_range(k, kl, ek, ekl)
	return self:contains_key_slice(k, kl) or self:contains_key_slice(ek, ekl)
end
function key_range_mt:intersects_range(kr)
	return self:contains(kr.s) or self:contains(kr.e)
end
function key_range_mt:__eq(kr)
	return kr.s == self.s and kr.e == self.e
end
function key_range_mt:__tostring()
--	local s = tostring(self.s)
--	s=s.."~"
--	return s..tostring(self.e)
	return ffi.string(self.s:as_slice()).."~"..ffi.string(self.e:as_slice())
end

ffi.metatype('luact_dht_key_range_t', key_range_mt)

return _M
