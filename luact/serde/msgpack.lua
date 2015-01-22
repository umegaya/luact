local ffi = require 'ffiex.init'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local socket = require 'pulpo.socket'
local tentacle = require 'pulpo.tentacle'

ffi.cdef [[
typedef enum luact_msgpack_object_type {
	LUACT_MSGPACK_ARRAY,
	LUACT_MSGPACK_MAP,
	LUACT_MSGPACK_STR,
	LUACT_MSGPACK_RAW,
	LUACT_MSGPACK_SINT,
	LUACT_MSGPACK_UINT,
	LUACT_MSGPACK_DOUBLE,
	LUACT_MSGPACK_FLOAT,
} luact_msgpack_object_type_t;
typedef enum luact_msgpack_ext_data_type {
	LUACT_EXT_ERROR,
	LUACT_EXT_FUNCTION,
	LUACT_EXT_CDATA_INT,
	LUACT_EXT_CDATA_FLOAT,
	LUACT_EXT_CDATA_TYPE,
	LUACT_EXT_CDATA_ARRAY,
} luact_msgpack_ext_data_type_t;
typedef struct luact_msgpack_object {
	uint32_t type;
	union {
		struct luact_msgpack_object *array;
		struct luact_msgpack_object_kv *map;
		char *str;
		uint8_t *raw;
		int64_t i;
		uint64_t u;
		double d;
		float f;
		void *cdata;
	};
} luact_msgpack_object_t;
typedef struct luact_msgpack_object_kv {
	luact_msgpack_object_t k, v;
} luact_msgpack_object_kv_t;
typedef struct luact_msgpack_serde {
	int dummy;
} luact_msgpack_serde_t;
]]
local ARRAY = ffi.cast('luact_msgpack_object_type_t', "LUACT_MSGPACK_ARRAY")
local MAP = ffi.cast('luact_msgpack_object_type_t', "LUACT_MSGPACK_MAP")
local STR = ffi.cast('luact_msgpack_object_type_t', "LUACT_MSGPACK_STR")
local RAW = ffi.cast('luact_msgpack_object_type_t', "LUACT_MSGPACK_RAW")
local SINT = ffi.cast('luact_msgpack_object_type_t', "LUACT_MSGPACK_SINT")
local UINT = ffi.cast('luact_msgpack_object_type_t', "LUACT_MSGPACK_UINT")
local DOUBLE = ffi.cast('luact_msgpack_object_type_t', "LUACT_MSGPACK_DOUBLE")
local FLOAT = ffi.cast('luact_msgpack_object_type_t', "LUACT_MSGPACK_FLOAT")

local EXT_ERROR = ffi.cast('luact_msgpack_ext_data_type_t', "LUACT_EXT_ERROR")
local EXT_FUNCTION = ffi.cast('luact_msgpack_ext_data_type_t', "LUACT_EXT_FUNCTION")
local EXT_CDATA_INT = ffi.cast('luact_msgpack_ext_data_type_t', "LUACT_EXT_CDATA_INT")
local EXT_CDATA_FLOAT = ffi.cast('luact_msgpack_ext_data_type_t', "LUACT_EXT_CDATA_FLOAT")
local EXT_CDATA_TYPE = ffi.cast('luact_msgpack_ext_data_type_t', "LUACT_EXT_CDATA_TYPE")

local cdata_map = {}


-- msgpack object
local msgpack_object_mt = {}
msgpack_object_mt.__index = msgpack_object_mt
function msgpack_object_mt:to_lua()

end

-- msgpack object (pure lua)


-- msgpack serde
local serde_mt = {}
serde_mt.__index = serde_mt
serde_mt.packer = {}
serde_mt.conv = ffi.new('luact_bytearray_scalar_conv_t')
function serde_mt.pack_length16(p, len)
	ffi.cast('uint16_t*', p)[0] = socket.htons(len)
	return 2
end
function serde_mt.pack_length32(p, len)
	ffi.cast('uint32_t*', p)[0] = socket.htonl(len)
	return 4
end
function serde_mt.packer.string(buf, p, str)
	local len, ofs = #str, 0
	buf:reserve(len + 4 + 1)
	if len <= 0x1F then
		p[0] = bit.bor(0xa0, len); ofs = 1
	elseif len <= 0xff then
		p[0], p[1] = 0xd9, len; ofs = 2
	elseif len <= 0xffff then
		p[0] = 0xda; ofs = 1
		ofs = ofs + serde_mt.pack_length16(p + ofs, len)
	else
		p[0] = 0xdb; ofs = 1
		ofs = ofs + serde_mt.pack_length32(p + ofs, len)
	end
	ofs = ofs + ffi.copy(p + ofs, str, len)
	return ofs
end
serde_mt.packer["error"] = function (buf, p, obj)

end
function serde_mt.packer.table(buf, p, obj)
	if exception.akin(obj) then
		return serde_mt["error"](buf, p, obj)
	end	
	local len, ofs = #obj
	buf:reserve(4 + 1)
	if len > 0 and (not obj.__not_array__) then
		if len <= 0xF then
			p[0] = bit.bor(0x90, len); ofs = 1
		elseif len <= 0xFFFF then
			p[0] = 0xdc; ofs = 1
			ofs = ofs + serde_mt.pack_length16(p + ofs, len)
		else
			p[0] = 0xdd; ofs = 1
			ofs = ofs + serde_mt.pack_length32(p + ofs, len)
		end
		-- array
		for i=1,len do
			ofs = ofs + serde_mt.pack_any(buf, p + ofs, obj[i])
		end
	else
		for k,v in pairs(obj) do
			len = len + 1
		end
		if len <= 0xF then
			p[0] = bit.bor(0x80, len); ofs = 1
		elseif len <= 0xFFFF then
			p[0] = 0xde; ofs = 1
			ofs = ofs + serde_mt.pack_length16(p + ofs, len)
		else
			p[0] = 0xdf; ofs = 1
			ofs = ofs + serde_mt.pack_length32(p + ofs, len)
		end
		-- map
		for k,v in pairs(obj) do
			ofs = ofs + serde_mt.pack_any(buf, p + ofs, k)
			ofs = ofs + serde_mt.pack_any(buf, p + ofs, v)
		end
	end
	return ofs
end
function serde_mt.packer.boolean(buf, p, obj)
	p[0] = obj and 0xc3 or 0xc2
	return 1
end
function serde_mt.packer.number(buf, p, obj)
	serde_mt.conv.d = obj
	p[0] = 0xcb
	ffi.copy(p + 1, serde_mt.conv.p, 8)
	return 9
end
serde_mt.packer["nil"] = function (buf, p, obj)
	p[0] = 0xc0
	return 1
end
serde_mt.packer["function"] = function (buf, p, obj) 
	local code = string.dump(obj)
	local len, ofs = #code
	if len < 0xFF then
		p[0], p[1], p[2] = 0xc7, len, EXT_TYPE_FUNC; ofs = 3
	elseif len < 0xFFFF then
		p[0] = 0xc8; ofs = 1
		ofs = serde_mt.pack_length16(p + ofs, len)
		p[ofs] = EXT_TYPE_FUNC; ofs = ofs + 1
	else
		p[0] = 0xc9; ofs = 1
		ofs = serde_mt.pack_length16(p + ofs, len)
		p[ofs] = EXT_TYPE_FUNC; ofs = ofs + 1
	end
	ofs = ofs + ffi.copy(p + ofs, code, len)
	return ofs
end
function serde_mt.packer.userdata(buf, p, obj)
	exception.raise('invalid', 'cannot pack', 'userdata')
end
function serde_mt.packer.thread(buf, p, obj)
	exception.raise('invalid', 'cannot pack', 'thread')
end
local custom_pack = {}
local custom_unpack = {}
function serde_mt.pack_int_cdata(buf, p, obj, refl)
	local ofs
	if refl.size == 1 then
		buf:reseve(4)
		p[0], p[1], p[2], p[3], ofs = 0xd4, EXT_CDATA_INT, refl.unsigned, obj, 4
	elseif refl.size == 2 then
		buf:reseve(3 + 2)
		p[0], p[1], p[2], ofs = 0xd5, EXT_CDATA_INT, refl.unsigned, 3
		ofs = ofs + serde_mt.pack_length16(p + ofs, obj)
	elseif refl.size == 4 then
		buf:reseve(3 + 4)
		p[0], p[1], p[2], ofs = 0xd6, EXT_CDATA_INT, refl.unsigned, 3
		ofs = ofs + serde_mt.pack_length32(p + ofs, obj)
	elseif refl.size == 8 then
		buf:reseve(3 + 8)
		p[0], p[1], p[2], ofs = 0xd7, EXT_CDATA_INT, refl.unsigned, 3
		ofs = ofs + ffi.copy(p + ofs, serde_mt.conv:unsigned2ptr(obj, refl), refl.size)
	else
		exception.raise('invalid', 'integer length', refl.size)
	end
	return ofs
end
function serde_mt.pack_struct_cdata(buf, p, obj, refl, length)
	local ctype_id, packer = serde_mt.get_ctype_id(refl.what, relf.name)
	-- print('refl.name = ', refl.name, 'size = ', refl.size)
	if packer then
		p[1] = EXT_CDATA_TYPE
		return 6 + packer(buf, p + 6, ctype_id, obj, length / refl.size)
	elseif refl.size <= 0xFF then
		buf:reserve(3 + 4 + length)
		p[0], p[1], p[2], ofs = 0xc7, EXT_CDATA_TYPE, refl.size, 3
		ofs = ofs + serde_mt.pack_length32(p + ofs, ctype_id)
	elseif refl.size <= 0xFFFF then			
		buf:reserve(2 + 2 + 4 + length)
		p[0], p[1], ofs = 0xc8, EXT_CDATA_TYPE, 2
		ofs = ofs + serde_mt.pack_length16(p + ofs, refl.size)
		ofs = ofs + serde_mt.pack_length32(p + ofs, ctype_id)
	else
		buf:reserve(2 + 4 + 4 + length)
		p[0], p[1], ofs = 0xc9, EXT_CDATA_TYPE, 2
		ofs = ofs + serde_mt.pack_length32(p + ofs, refl.size)
		ofs = ofs + serde_mt.pack_length32(p + ofs, ctype_id)
	end
	return ofs + ffi.copy(p + ofs, obj, length)
end
function serde_mt.packer.cdata(buf, p, obj)
	local ofs
	local refl = reflect.typeof(obj)
	if refl.what == 'int' or refl.what == 'enum' then
		return serde_mt.pack_int_cdata(p, obj, refl)
	elseif refl.what == 'float' then
		if refl.size == 4 then
			p[0], p[1], ofs = 0xd6, EXT_CDATA_FLOAT, 2
		elseif refl.size == 8 then
			p[0], p[1], ofs = 0xd7, EXT_CDATA_FLOAT, 2
		else
			exception.raise('invalid', 'float length', refl.size)
		end
		return ofs + ffi.copy(p + ofs, self:float2ptr(obj, refl), refl.size)
	elseif refl.what == 'struct' or refl.what == 'union' then
		return serde_mt.pack_struct_cdata(p, obj, refl)
	elseif refl.what == 'array' then
		local et = refl.element_type
		local size = (refl.size == 'none' and ffi.sizeof(obj) or refl.size)
		return serde_mt.pack_struct_cdata(p, obj, et, size)
	elseif refl.what == 'ptr' or refl.what == 'ref' then
		local et = refl.element_type
		if et.name then -- struct/union
			return serde_mt.pack_struct_cdata(p, obj, et, refl.size)
		else
			return serde_mt.pack_int_cdata(p, obj, refl)
		end
	end
end
function serde_mt.pack_any(buf, p, obj)
	return self.packer[type(obj)](buf, p, obj)
end
function serde_mt:pack_packet(buf, append, ...)
	pv = ffi.cast('luact_writer_raw_t*', buf:curr_p())
	local args = {...}
	if append then
		local sz = pv.sz
		for i=1,#args do
			pv.sz = pv.sz + self.pack_any(buf, pv.p + pv.sz, args[i])
		end
		return pv.sz - sz
	else
		pv.sz = 0
		for i=1,#args do
			pv.sz = pv.sz + self.pack_any(buf, pv.p + pv.sz, args[i])
		end
		return pv.sz
	end
end
function serde_mt:pack(buf, obj)
	return self.pack_any(buf, buf:curr_p(), obj)
end
-- unpack
function serde_mt.wait_data_arrived(rb, p, remain, required)
	while remain < required do
		rb:use(p, rb:curr_p())
		coroutine.yield()
		p, remain = rb:curr_p(), rb:available()
	end
	return p, remain
end
function serde_mt.unpack_map(rb, p, remain, size)
	local r = {}
	for i=1,size do
		k, p, remain = serde_mt.unpack_any(rb, p, remain)
		v, p, remain = serde_mt.unpack_any(rb, p, remain)
		r[k] = v
	end
	return r, p, remain
end
function serde_mt.unpack_array(rb, p, remain, size)
	local r = {}
	for i=1,size do
		v, p, remain = serde_mt.unpack_any(rb, p, remain)
		table.insert(r, v)
	end
	return r, p, remain
end
function serde_mt.unpack_int(rb, p, remain, size)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, size)
	return p[0], p + 1, remain - 1
end
function serde_mt.unpack_ext_struct_cdata(rb, p, remain, size)
	local ofs = 2 + 4 + size
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, ofs)
	local len = serde_mt.conv:ptr2unsigned(p + 1, size)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, ofs + len)
	local ctype_id = socket.get32(p)
	local ct = serde_mt.get_ctype_from_id(ctype_id)
	local ptr = memory.managed_alloc_typed(ct, size / ffi.sizeof(ct))
	ffi.copy(ptr, p + ofs, len)
	return ptr, p + ofs + len, remain - ofs - len
end
function serde_mt.unpack_ext_numeric_cdata(rb, p, remain, size)
	local ofs = 3 + size
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, ofs)
	if p[1] == EXT_CDATA_INT then
		if p[2] ~= 0 then
			return serde_mt.conv:ptr2unsigned(p + 3, size), p + ofs, remain - ofs
		else
			return serde_mt.conv:ptr2signed(p + 3, size), p + ofs, remain - ofs
		end
	elseif p[1] == EXT_CDATA_FLOAT then
		if p[2] ~= 0 then
			return serde_mt.conv:ptr2float(p + 3, size), p + ofs, remain - ofs
		else
			return serde_mt.conv:ptr2float(p + 3, size), p + ofs, remain - ofs
		end
	else
		exception.raise('invalid', 'currently, fixext always represent numeric cdata')
	end		
end

function serde_mt.unpack_int(rb, p, remain, size)
	local ofs = 1 + size
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, ofs)
	return serde_mt.conv:ptr2signed(p, size), p + ofs, remain - ofs
end
function serde_mt.unpack_uint(rb, p, remain, size)
	local ofs = 1 + size
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, ofs)
	return serde_mt.conv:ptr2unsigned(p, size), p + ofs, remain - ofs
end
function serde_mt.unpack_bin(rb, p, remain, size)
	local ofs = 1 + size
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, ofs)
	local len = serde_mt.conv:ptr2unsigned(p + 1, size)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, ofs + len)
	return ffi.string(p + ofs, len), p + ofs + len, remain - ofs - len
end

function serde_mt.unpack_fixint(rb, p, remain)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, 1)
	return tonumber(p[0]), p + 1, remain - 1
end
function serde_mt.unpack_negative_fixint(rb, p, remain)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, 1)
	return tonumber(ffi.cast('char', p[0])), p + 1, remain - 1
end
function serde_mt.unpack_str(rb, p, remain, size)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, size)
	return ffi.string(p, size), p + size, remain - size
end


function serde_mt.unpack_fixmap(rb, p, remain)
	local size = tonumber(p[0]) - 0x80
	return serde_mt.unpack_map(p + 1, remain - 1, size)
end
function serde_mt.unpack_fixarray(rb, p, remain)
	local size = tonumber(p[0]) - 0x90
	return serde_mt.unpack_array(p + 1, remain - 1, size)
end
function serde_mt.unpack_fixstr(rb, p, remain)
	local size = tonumber(p[0]) - 0xa0
	return serde_mt.unpack_str(p + 1, remain - 1, size)	
end

function serde_mt.unpack_nil(rb, p, remain)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, 1)
	return nil, p + 1, remain - 1
end
function serde_mt.unpack_true(rb, p, remain)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, 1)
	return true, p + 1, remain - 1
end
function serde_mt.unpack_false(rb, p, remain)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, 1)
	return false, p + 1, remain - 1
end

function serde_mt.unpack_bin8(rb, p, remain)
	return serde_mt.unpack_bin(rb, p, remain, 1)
end
function serde_mt.unpack_bin16(rb, p, remain)
	return serde_mt.unpack_bin(rb, p, remain, 2)
end
function serde_mt.unpack_bin32(rb, p, remain)
	return serde_mt.unpack_bin(rb, p, remain, 4)
end

function serde_mt.unpack_ext8(rb, p, remain)
	return serde_mt.unpack_ext_struct_cdata(rb, p, remain, 1)
end
function serde_mt.unpack_ext16(rb, p, remain)
	return serde_mt.unpack_ext_struct_cdata(rb, p, remain, 2)
end
function serde_mt.unpack_ext32(rb, p, remain)
	return serde_mt.unpack_ext_struct_cdata(rb, p, remain, 4)
end

function serde_mt.unpack_fixext8(rb, p, remain)
	return serde_mt.unpack_ext_numeric_cdata(rb, p, remain, 1)
end
function serde_mt.unpack_fixext16(rb, p, remain)
	return serde_mt.unpack_ext_numeric_cdata(rb, p, remain, 2)
end
function serde_mt.unpack_fixext32(rb, p, remain)
	return serde_mt.unpack_ext_numeric_cdata(rb, p, remain, 4)
end
function serde_mt.unpack_fixext64(rb, p, remain)
	return serde_mt.unpack_ext_numeric_cdata(rb, p, remain, 8)
end
function serde_mt.unpack_fixext128(rb, p, remain)
	return serde_mt.unpack_ext_numeric_cdata(rb, p, remain, 16)
end


function serde_mt.unpack_uint8(rb, p, remain)
	return serde_mt.unpack_uint(rb, p, remain, 1)
end
function serde_mt.unpack_uint16(rb, p, remain)
	return serde_mt.unpack_uint(rb, p, remain, 2)
end
function serde_mt.unpack_uint32(rb, p, remain)
	return serde_mt.unpack_uint(rb, p, remain, 4)
end
function serde_mt.unpack_uint64(rb, p, remain)
	return serde_mt.unpack_uint(rb, p, remain, 8)
end

function serde_mt.unpack_int8(rb, p, remain)
	return serde_mt.unpack_int(rb, p, remain, 1)
end
function serde_mt.unpack_int16(rb, p, remain)
	return serde_mt.unpack_int(rb, p, remain, 2)
end
function serde_mt.unpack_int32(rb, p, remain)
	return serde_mt.unpack_int(rb, p, remain, 4)
end
function serde_mt.unpack_int64(rb, p, remain)
	return serde_mt.unpack_int(rb, p, remain, 8)
end

function serde_mt.unpack_str8(rb, p, remain)
	return serde_mt.unpack_bin(rb, p, remain, 1)
end
function serde_mt.unpack_str16(rb, p, remain)
	return serde_mt.unpack_bin(rb, p, remain, 2)
end
function serde_mt.unpack_str32(rb, p, remain)
	return serde_mt.unpack_bin(rb, p, remain, 4)
end

function serde_mt.unpack_array16(rb, p, remain)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, 3)
	local len = serde_mt.conv:ptr2unsigned(p + 1, 2)
	return serde_mt.unpack_array(p, remain, len)
end
function serde_mt.unpack_array32(rb, p, remain)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, 5)
	local len = serde_mt.conv:ptr2unsigned(p + 1, 4)
	return serde_mt.unpack_array(p, remain, len)
end

function serde_mt.unpack_map16(rb, p, remain)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, 3)
	local len = serde_mt.conv:ptr2unsigned(p + 1, 2)
	return serde_mt.unpack_map(p, remain, len)
end
function serde_mt.unpack_map32(rb, p, remain)
	p, remain = serde_mt.wait_data_arrived(rb, p, remain, 5)
	local len = serde_mt.conv:ptr2unsigned(p + 1, 4)
	return serde_mt.unpack_map(p, remain, len)
end


local unpacker = {}
serde_mt.unpacker = unpacker
for i=0x00,0x7f do
	unpacker[i] = serde_mt.unpack_fixint
end
for i=0x80,0x8f do
	unpacker[i] = serde_mt.unpack_fixmap
end
for i=0x90,0x9f do
	unpacker[i] = serde_mt.unpack_fixarray
end
for i=0xa0,0xbf do
	unpacker[i] = serde_mt.unpack_fixstr	
end
for i=0xe0,0xff do
	unpacker[i] = serde_mt.unpack_negative_fixint
end
unpacker[0xc0] = serde_mt.unpack_nil
unpacker[0xc2] = serde_mt.unpack_false
unpacker[0xc3] = serde_mt.unpack_true

unpacker[0xc4] = serde_mt.unpack_bin8
unpacker[0xc5] = serde_mt.unpack_bin16
unpacker[0xc6] = serde_mt.unpack_bin32

unpacker[0xc7] = serde_mt.unpack_ext8
unpacker[0xc8] = serde_mt.unpack_ext16
unpacker[0xc9] = serde_mt.unpack_ext32

unpacker[0xca] = serde_mt.unpack_float32
unpacker[0xcb] = serde_mt.unpack_float64

unpacker[0xcc] = serde_mt.unpack_uint8
unpacker[0xcd] = serde_mt.unpack_uint16
unpacker[0xce] = serde_mt.unpack_uint32
unpacker[0xcf] = serde_mt.unpack_uint64

unpacker[0xd0] = serde_mt.unpack_int8
unpacker[0xd1] = serde_mt.unpack_int16
unpacker[0xd2] = serde_mt.unpack_int32
unpacker[0xd3] = serde_mt.unpack_int64

unpacker[0xd4] = serde_mt.unpack_fixext8
unpacker[0xd5] = serde_mt.unpack_fixext16
unpacker[0xd6] = serde_mt.unpack_fixext32
unpacker[0xd7] = serde_mt.unpack_fixext64
unpacker[0xd8] = serde_mt.unpack_fixext128

unpacker[0xd9] = serde_mt.unpack_str8
unpacker[0xda] = serde_mt.unpack_str16
unpacker[0xdb] = serde_mt.unpack_str32

unpacker[0xdc] = serde_mt.unpack_array16
unpacker[0xdd] = serde_mt.unpack_array32

unpacker[0xde] = serde_mt.unpack_map16
unpacker[0xdf] = serde_mt.unpack_map32

function serde_mt.unpack_any(rb, p, remain)
	return serde_mt.unpacker[p[0]](rb, p, remain)
end

function serde_mt:start_unpacker(rb)
	local r, p, remain
	while true do
		p, remain = rb:curr_p(), rb:available()
		while remain > 0 do
			r, p, remain = self.unpack_any(rb, p, remain)
			coroutine.yield(true, r)
		end
	end
end
function serde_mt:stream_unpacker(rb)
	return coroutine.create(self.start_unpacker, self, rb)
end
function serde_mt:unpack_packet(ctx)
	local ok, fin, r = coroutine.resume(ctx)
	if ok then
		return r, nil, fin
	else
		return nil, fin
	end
end
function serde_mt:unpack(rb)
	return serde_mt.unpack_any(rb:curr_p(), rb:available())
end
serde_mt.unpack_packet = serde_mt.unpack
function serde_mt:customize(ctype, pack, unpack)
end
ffi.metatype('luact_msgpack_serde_t', serde_mt)

return memory.alloc_typed('luact_msgpack_serde_t')





