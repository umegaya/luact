local ffi = require 'ffiex.init'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'


-- module table
local _M = {}


-- constant
_M.MAX_LENGTH = 255


-- cdefs
ffi.cdef(([[
typedef struct luact_dht_key {
	uint8_t length;
	union {
		uint8_t data[%d];
		uint8_t p[0];
	};
} luact_dht_key_t;
]]):format(_M.MAX_LENGTH))


-- constant cdata
_M.MIN = memory.alloc_fill_typed('luact_dht_key_t')
_M.MIN.length = 0
_M.MAX = memory.alloc_fill_typed('luact_dht_key_t', 0xFF, _M.MAX_LENGTH)
_M.MAX.length = _M.MAX_LENGTH


-- key
local key_mt = {}
key_mt.__index = key_mt
function key_mt:init(k, kl)
	kl = kl or (k and #k or 0)
	if kl > _M.MAX_LENGTH then
		exception.raise('invalid', 'too long key', kl)
	end
	self.length = kl
	if kl > 0 then
		ffi.copy(self.data, k, kl)
	end
	return self
end
function key_mt:copy_to(buf)
	ffi.copy(buf, self, self.length + 1)
end
--[[
	=>: prev, <=: next
	ff:ff:ff:ff 		<=> ff:ff:ff:fe:ff...
	ff:ff:ff:01 		<=> ff:ff:ff:00:ff...
	ff:ff:fe:ff... 		<=> ff:ff:fe:ff...:fe
	ff:ff:fe:ff...:00 	<=> ff:ff:fe:ff...(remove trailing zero)
	ff:ff:fe:ff... (max length - 1) <=> ff:ff:fe:ff:...:fe:ff (max length)
	01:00...			<=> 01:00... (length - 1)
	00...				<=> 00... (length - 1)
	00 					<=> ""
]]
function key_mt:prev(buf)
	self:copy_to(buf)
	buf:dec()
	return buf
end
function key_mt:dec()
	local idx, v = self.length - 1
	if idx >= 0 then
		local v = self.p[idx]
		if v > 0 then
			self.p[idx] = v - 1
			ffi.fill(self.p + idx + 1, _M.MAX_LENGTH - idx - 1, 0xFF)
			self.length = _M.MAX_LENGTH
		else -- remove trail 0 
			self.length = self.length - 1
		end
	else
		exception.raise('invalid', 'this is min key')
	end
	return self
end
function key_mt:next(buf)
	self:copy_to(buf)
	buf:inc()
	return buf
end

function key_mt:inc()
	if self.length < _M.MAX_LENGTH then
		-- add trailing zero
		self.p[self.length] = 0x00
		self.length = self.length + 1
		return self
	end
	local idx, v = self.length - 1
::AGAIN::
	v = self.p[idx]
	if v < 0xFF then
		self.p[idx] = v + 1
		-- fill by zero at the end of key buffer
		self.length = idx + 1
	elseif idx > 0 then
		idx = idx - 1
		goto AGAIN
	else -- all bytes are 0xFF
		exception.raise('invalid', 'this is max key')		
	end
	return self
end
function key_mt:equals(k, kl)
	if self.length ~= kl then
		return false
	else
		return memory.cmp(self.p, k, self.length)
	end
end
function key_mt:less_than(k, kl)
	if self.length <= 0 then -- min key
		return kl > 0
	elseif kl <= 0 then -- k is min key
		return false
	elseif self.length < kl then
		return memory.rawcmp(self.p, k, self.length) <= 0 
	elseif self.length >= kl then
		return memory.rawcmp(self.p, k, kl) < 0
	end
end
function key_mt:less_than_equals(k, kl)
	if self.length <= 0 then -- min key
		return true
	elseif kl <= 0 then -- k is min key
		return self.length <= 0
	elseif self.length <= kl then
		return memory.rawcmp(self.p, k, self.length) <= 0 
	elseif self.length > kl then
		return memory.rawcmp(self.p, k, kl) < 0
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
function key_mt:__tostring()
	local len = tonumber(self.length)
	local s = "#"..tostring(len)
	for i=0,len-1 do
		s=s..(":%02x"):format(self.p[i])
	end
	return s
end
ffi.metatype('luact_dht_key_t', key_mt)

return _M
