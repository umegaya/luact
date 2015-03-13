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

return _M
