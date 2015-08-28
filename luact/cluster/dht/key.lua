local ffi = require 'ffiex.init'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local buffer = require 'luact.util.buffer'
local util = require 'pulpo.util'
local const = require 'luact.cluster.dht.const'


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

typedef struct luact_dht_key_codec {
	uint16_t size, idx;
	uint32_t *lengths;
	char **buffers;
} luact_dht_key_codec_t;

typedef struct luact_dht_kind {
	char prefix[2];
	uint8_t txnl, padd;
} luact_dht_kind_t;
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
	if ekl > 0 then
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


-- mvcc encode/decode key
local key_codec_mt = {}
key_codec_mt.__index = key_codec_mt
function key_codec_mt:init(size)
	self.size = size
	self.idx = 0
	self.lengths = memory.alloc_fill_typed('uint32_t', size)
	self.buffers = memory.alloc_fill_typed('char *', size)
	for i=0, self.size - 1 do
		self.lengths[i] = 256
		self.buffers[i] = memory.alloc(self.lengths[i])
	end
end
function key_codec_mt:reserve(k, kl, ts)
	-- +1 for using in self:next_of
	local reqlen = (util.encode_binary_length(kl + (ts and ffi.sizeof(ts) or 0)) + 1)
	local curidx = self.idx
	local len = self.lengths[curidx]
	if len >= reqlen then 
		self.idx = (self.idx + 1) % self.size
		return self.buffers[curidx], len 
	end
	while len < reqlen do
		len = len * 2
	end
	-- print('---------------- reserve: newlen', len, kl, ffi.sizeof('pulpo_hlc_t'))
	local tmp = memory.realloc_typed('char', self.buffers[curidx], len)
	if tmp ~= ffi.NULL then
		self.buffers[curidx] = tmp
		self.lengths[curidx] = len
		self.idx = (self.idx + 1) % self.size
		return tmp, len
	else
		exception.raise('fatal', 'malloc', len)
	end
end
function key_codec_mt:encode(k, kl, ts)
	local _, olen, ofs
	local p, len = self:reserve(k, kl, ts)
	_, ofs = util.encode_binary(k, kl, p, len)
	if ts then
		-- TODO : should consider endian-ness
		-- TODO : better using # operator instead of ffi.sizeof?
		_, olen = util.encode_binary(ts.p, ffi.sizeof(ts), p + ofs, len - ofs)
		ofs = ofs + olen
	end
	return p, ofs
end
function key_codec_mt:encode_coloc(coloc_type, k, kl, detail)
	local _, olen, ofs
	local p, len = self:reserve(k, kl + 2, detail) -- +2 for coloc prefix
	p[0], p[1] = 0, coloc_type
	_, ofs = util.encode_binary(k, kl, p + 2, len - 2)
	ofs = ofs + 2
	if detail then
		-- TODO : should consider endian-ness
		-- TODO : better using # operator instead of ffi.sizeof?
		_, olen = util.encode_binary(detail, ffi.sizeof(detail), p + ofs, len - ofs)
		ofs = ofs + olen
	end
	return p, ofs
end
function key_codec_mt:decode(ek, ekl, ctype)
	local p, len = self:reserve(ek, ekl, ctype)
	local k, kl, n_read = util.decode_binary(ek, ekl, p, len)
	if n_read >= ekl then
		return k, kl
	else
		-- +1 for additional \0 when this pointer called with next_of
		local ts, tsl = util.decode_binary(ek + n_read, ekl - n_read, p + kl + 1, len - kl - 1)
		return k, kl, ffi.cast(ctype or 'pulpo_hlc_t *', ts)
	end
end
function key_codec_mt:available(p, pl)
	for i=0, self.size - 1 do
		if self.buffers[i] == p then
			if (self.lengths[i] - pl) > 0 then
				return true
			else
				logger.report('error', 'allocated pointer length short: try recover')
				local tmp = memory.realloc_typed('char', p, pl)
				if tmp == ffi.NULL then
					exception.raise('fatal', 'realloc', pl, p)
				end
				self.buffers[i] = tmp
				return true
			end
		end
	end
end
-- caluculate next key by appending \0 to last 
function key_codec_mt:next_of(k, kl)
	if self:available(k, kl) then
		k[kl] = 0
		return k, kl + 1
	else
		local p, len = self:reserve(k, kl)
		ffi.copy(p, k, kl)
		return self:next_of(p, kl)
	end
end
function key_codec_mt:upper_bound_of_prefix(k, kl)
	local rk, rkl = self:reserve(k, kl)
	ffi.copy(rk, k, kl)
	for i=kl-1,0,-1 do
		rk[i] = rk[i] + 1
		if rk[i] ~= 0 then
			return rk, rkl
		end
	end
	return rk, rkl
end
function key_codec_mt:timestamp_of(iter)
	local k, kl, ts = self:decode(iter:key())
	return ts
end
ffi.metatype('luact_dht_key_codec_t', key_codec_mt)


-- meta key destribution
--[[
### [min]    : unused

### [\0, \1) : un-splittable system keys (configuration) => つまり全てroot rangeのノードに保持される
```
[\0\0, \0\1) : kind (独立したdhtを管理する単位)の名前   \0\0 + numerical id => kind name これによってnumerical id (within 1byte)が予約される
[\0\1, \1  ) : reserved
```

### [\1, \40) : splittable system keys => 数の多いconfiguration, あるいはtxnのいらないユーザーデータ 
```
[\1, \2 ) : vid (仮想アクターのidと物理アドレスのマッピング) のmeta2 ranges \2 + key, non-transactional
[\2, \3 ) : vid state (仮想アクターの永続化される状態,{vid+key}と{value}のマッピング) のmeta2 ranges \3 + key, transactional
[\3, \40) : reserved
```

### [\40,\ff) : splittable user keys (max 180)
```
[\40,\41) : user data : category 1のmeta2 ranges \40 + key
[\41,\42) : user data : category 2のmeta2 ranges \41 + key
...
[\fe,\ff) : user data : category 180のmeta2 ranges \fe + key
```
]]
local function new_kind(prefix, txnl)
	local p = memory.alloc_typed('luact_dht_kind_t') -- cdef is done in range_cdef.lua
	ffi.copy(p.prefix, prefix, #prefix)
	p.txnl = txnl
	return p
end
local kind_map = {
	-- system category (built in)
	[const.KIND_META1] = new_kind("", true),
	[const.KIND_META2] = new_kind("", true),
	[const.KIND_VID] = new_kind(string.char(1), false),
	[const.KIND_STATE] = new_kind(string.char(2), true),
}


-- module functions
-- codec
_M.codec = memory.alloc_typed('luact_dht_key_codec_t')
_M.codec:init(32)


-- key kind
-- txnl : use transactional operation or not
-- *caution* txnl==false does not means split does not need transaction. 
-- range data of txnl == false still need txn to split. 
function _M.create_kind(id, txnl)
	kind_map[id] = new_kind(string.char(id), txnl)
end
-- check specified *kind* of range is transactional (has version value)
function _M.is_mvcc(kind)
	return kind_map[kind].txnl ~= 0
end


-- key generation helper
-- TODO : use statical memory buffer and copy buffer
function _M.make_arbiter_id(kind, k, kl)
	return ffi.string(k, kl)
end
function _M.make_metakey(kind, k, kl)
	if kind > const.KIND_META2 then
		kind = kind_map[kind]
		local p = memory.managed_alloc_typed('char', kl + 1)
		ffi.copy(p, kind.prefix, 1)
		ffi.copy(p + 1, k, kl)
		return p, kl + 1
	else
		return k, kl
	end
end
function _M.make_syskey(category, k, kl)
	return string.char(0, category)..ffi.string(k, kl)
end
function _M.make_user_kind_syskey(k, kl)
	return make_syskey(const.SYSKEY_CATEGORY_KIND, k, kl)
end
local coloc_detail_cdata = {
	[const.COLOC_TXN] = 'luact_uuid_t *',
}
function _M.make_coloc_key(k, kl, coloc_type, detail)
	assert(kl > 0, "coloc_key invalid")
	return _M.codec:encode_coloc(coloc_type, k, kl, detail)
end
function _M.make_txn_key(txn)
	return _M.make_coloc_key(txn.key, txn.kl, const.COLOC_TXN, txn.id)
end

function _M.parse_if_coloc_key(k, kl)
	local coloc, coloc_type
	if kl then
		coloc, coloc_type = (k[0] == 0), k[1]
	else
		coloc, coloc_type = (k:byte() == 0), k:byte(2)
	end
	if coloc then
		return coloc, _M.codec:decode(ffi.cast('uint8_t *', k) + 2, kl - 2, coloc_detail_cdata[coloc_type])
	else
		return false, _M.codec:decode(k, kl)
	end
end
function _M.inspect(k, kl)
	local coloc, dk, dkl, detail = _M.parse_if_coloc_key(k, kl)
	local src = {'key:('..tostring(tonumber(kl))..')'..tostring(k)}
	for i=0,dkl-1 do
		table.insert(src, (':%02x'):format(ffi.cast('const unsigned char *', dk)[i]))
	end
	if detail then
		table.insert(src, (' @ ')..tostring(detail))
	end
	return table.concat(src)
end


return _M
