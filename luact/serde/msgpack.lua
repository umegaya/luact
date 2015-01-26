-- TODO : treat correctly the case client machine is not little endian.
local ffi = require 'ffiex.init'
local reflect = require 'reflect'
local common = require 'luact.serde.common'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local socket = require 'pulpo.socket'
local tentacle = require 'pulpo.tentacle'
local writer = require 'luact.writer'

local WRITER_RAW = writer.WRITER_RAW

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
function serde_mt.packer.string(buf, str)
	local len, ofs, p = #str, 0
	buf:reserve(len + 4 + 1)
	p = buf:last_byte_p()
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
	ffi.copy(p + ofs, str, len)
	ofs = ofs + len
	buf:use(ofs)
end
serde_mt.packer["error"] = function (buf, obj)
	local p, ofs
	buf:reserve(2)
	p = buf:last_byte_p()
	p[0], p[1] = 0xc9, EXT_ERROR
	buf:use(2)
	ofs = buf.used
	buf:use(4) -- reserve for length
	serde_mt.pack_any(buf, obj.name)
	serde_mt.pack_any(buf, obj.bt)
	serde_mt.pack_any(buf, obj.args)
	-- only in here, length can be known
	serde_mt.pack_length32(buf:start_p() + ofs, buf.used - ofs - 4)
end
function serde_mt.packer.table(buf, obj)
	if exception.akin(obj) then
		return serde_mt.packer["error"](buf, obj)
	end	
	local len, ofs, p = #obj
	buf:reserve(4 + 1)
	p = buf:last_byte_p()
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
		buf:use(ofs)
		for i=1,len do
			serde_mt.pack_any(buf, obj[i])
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
		buf:use(ofs)
		for k,v in pairs(obj) do
			serde_mt.pack_any(buf, k)
			serde_mt.pack_any(buf, v)
		end
	end
end
function serde_mt.packer.boolean(buf, obj)
	local p
	buf:reserve(1)
	p = buf:last_byte_p()
	p[0] = obj and 0xc3 or 0xc2
	buf:use(1)
end
function serde_mt.packer.number(buf, obj)
	local p
	buf:reserve(9)
	p = buf:last_byte_p()
	serde_mt.conv.d = obj
	p[0] = 0xcb
	ffi.copy(p + 1, serde_mt.conv.p, 8)
	buf:use(9)
end
serde_mt.packer["nil"] = function (buf, obj)
	local p
	buf:reserve(1)
	p = buf:last_byte_p()
	p[0] = 0xc0
	buf:use(1)
end
function serde_mt.pack_ext_header(buf, ext_type, length)
	local p
	local ofs = 0
	length = length + 1
	if length <= 0xFF then
		buf:reserve(3 + length)
		p = buf:last_byte_p()
		p[0], p[1], p[2], ofs = 0xc7, length, ext_type, 3
	elseif length <= 0xFFFF then			
		buf:reserve(4 + length)
		p = buf:last_byte_p()
		p[0], ofs = 0xc8, 1
		ofs = ofs + serde_mt.pack_length16(p + ofs, length)
		p[ofs] = ext_type; ofs = ofs + 1
	else
		buf:reserve(6 + length)
		p = buf:last_byte_p()
		p[0], ofs = 0xc9, 1
		ofs = ofs + serde_mt.pack_length32(p + ofs, length)
		p[ofs] = ext_type; ofs = ofs + 1
	end
	return p, ofs
end
serde_mt.packer["function"] = function (buf, obj) 
	local code = string.dump(obj)
	local len, ofs, p = #code
	p, ofs = serde_mt.pack_ext_header(buf, EXT_FUNCTION, len)
	ffi.copy(p + ofs, code, len)
	ofs = ofs + len
	buf:use(ofs)
end
function serde_mt.packer.userdata(buf, obj)
	exception.raise('invalid', 'cannot pack', 'userdata')
end
function serde_mt.packer.thread(buf, obj)
	exception.raise('invalid', 'cannot pack', 'thread')
end
local custom_pack = {}
local custom_unpack = {}
function serde_mt.pack_int_cdata(buf, obj, refl)
	local ofs, p
	if refl.size == 1 then
		buf:reserve(4)
		p = buf:last_byte_p()
		p[0], p[1], p[2], p[3], ofs = 0xd4, EXT_CDATA_INT, refl.unsigned or 0, obj, 4
	elseif refl.size == 2 then
		buf:reserve(3 + 2)
		p = buf:last_byte_p()
		p[0], p[1], p[2], ofs = 0xd5, EXT_CDATA_INT, refl.unsigned or 0, 3
		ofs = ofs + serde_mt.pack_length16(p + ofs, obj)
	elseif refl.size == 4 then
		buf:reserve(3 + 4)
		p = buf:last_byte_p()
		p[0], p[1], p[2], ofs = 0xd6, EXT_CDATA_INT, refl.unsigned or 0, 3
		ofs = ofs + serde_mt.pack_length32(p + ofs, obj)
	elseif refl.size == 8 then
		buf:reserve(3 + 8)
		p = buf:last_byte_p()
		p[0], p[1], p[2], ofs = 0xd7, EXT_CDATA_INT, refl.unsigned or 0, 3
		ffi.copy(p + ofs, serde_mt.conv:unsigned2ptr(obj, refl), refl.size)
		ofs = ofs + refl.size
	else
		exception.raise('invalid', 'integer length', refl.size)
	end
	buf:use(ofs)
end
function serde_mt.pack_ext_cdata_header(buf, length, ctype_id)
	local p, ofs = serde_mt.pack_ext_header(buf, EXT_CDATA_TYPE, length + 4)
	ofs = ofs + serde_mt.pack_length32(p + ofs, ctype_id)
	return p, ofs
end
function serde_mt.pack_struct_cdata(buf, obj, refl, length)
	local ctype_id = common.ctype_id(refl.what, refl.name)
	local packer = common.msgpack_packer[ctype_id]
	local ofs = 0
	local p
	if packer then
		buf:reserve(2 + 4 + 4) -- type/ext type/cdata_id/length
		p = buf:last_byte_p()
		p[1] = EXT_CDATA_TYPE
		buf:use(packer(serde_mt, buf, ctype_id, obj, length))
		return
	else
		p, ofs = serde_mt.pack_ext_cdata_header(buf, length, ctype_id)
	end
	ffi.copy(p + ofs, obj, length)
	ofs = ofs + length
	buf:use(ofs)
end
--[[
CDATA serialize/unserialize rule:
int/enum => int
float/double => float/double (no change)
struct/union => ref of struct/union
array => array 
ptr/ref => ref (even if ptr has multiple element of ctype)
]]
function serde_mt.packer.cdata(buf, obj)
	local ofs, p = 0
	local refl = reflect.typeof(obj)
	if refl.what == 'int' or refl.what == 'enum' then
		return serde_mt.pack_int_cdata(buf, obj, refl)
	elseif refl.what == 'float' then
		buf:reserve(3)
		p = buf:last_byte_p()
		if refl.size == 4 then
			p[0], p[1], ofs = 0xd6, EXT_CDATA_FLOAT, 2
		elseif refl.size == 8 then
			p[0], p[1], ofs = 0xd7, EXT_CDATA_FLOAT, 2
		else
			exception.raise('invalid', 'float length', refl.size)
		end
		ffi.copy(p + ofs, self:float2ptr(obj, refl), refl.size)
		ofs = ofs + refl.size
		buf:use(ofs)
	elseif refl.what == 'struct' or refl.what == 'union' then
		serde_mt.pack_struct_cdata(buf, obj, refl, refl.size)
	elseif refl.what == 'array' then
		local et = refl.element_type
		local size = (refl.size == 'none' and ffi.sizeof(obj) or refl.size)
		serde_mt.pack_struct_cdata(buf, obj, et, size)
	elseif refl.what == 'ptr' or refl.what == 'ref' then
		local et = refl.element_type
		if et.name then -- struct/union
			serde_mt.pack_struct_cdata(buf, obj, et, et.size)
		else
			serde_mt.pack_int_cdata(buf, obj, refl)
		end
	end
end
function serde_mt.pack_any(buf, obj)
	return serde_mt.packer[type(obj)](buf, obj)
end
function serde_mt:pack_packet(buf, append, ...)
	local hdsz = ffi.sizeof('luact_writer_raw_t')
	local args = {...}
	if append then
		local sz = buf.used
		--for i=1,#args do
		--	logger.warn('pack2', i, args[i])
		--end
		serde_mt.pack_any(buf, args)
		sz = buf.used - sz
		local pv = ffi.cast('luact_writer_raw_t*', buf:curr_p())
		pv.sz = pv.sz + sz
		--buf:dump()
		return sz
	else
		buf:reserve_with_cmd(hdsz, WRITER_RAW)
		-- allocate size for header (luact_writer_raw_t)
		buf:use(hdsz)
		local sz = buf.used
		--for i=1,#args do
		--	logger.warn('pack', i, args[i])
		--end
		serde_mt.pack_any(buf, args)
		local pv = ffi.cast('luact_writer_raw_t*', buf:curr_p())
		pv.sz = buf.used - sz
		pv.ofs = 0
		--buf:dump()
		return pv.sz
	end
end
function serde_mt:pack(buf, obj)
	return self.pack_any(buf, obj)
end
-- unpack
function serde_mt.wait_data_arrived(rb, required)
	-- logger.info('rrqe', rb:available(), required)
	while rb:available() < required do
		--logger.info('wait_data_arrived', rb:available(), required)
		coroutine.yield()
	end
	return rb:curr_byte_p()
end
function serde_mt.unpack_map(rb, size)
	local r, k, v = {}
	for i=1,size do
		k = serde_mt.unpack_any(rb)
		v = serde_mt.unpack_any(rb)
		--logger.notice('unpack_map', i, size, k, v)
		r[k] = v
	end
	return r
end
function serde_mt.unpack_array(rb, size)
	local r = {}
	for i=1,size do
		local v = serde_mt.unpack_any(rb)
		--logger.warn('unpack_array', i, size, v)
		table.insert(r, v)
	end
	return r
end
function serde_mt.unpack_ext_struct_cdata(rb, size)
	local p = serde_mt.wait_data_arrived(rb, 1 + size)
	local len = serde_mt.conv:ptr2unsigned(p + 1, size)
	rb:seek_from_curr(1 + size)
	p = serde_mt.wait_data_arrived(rb, len)
	local ctype_id = socket.get32(p + 1) -- +1 for skip ext_type
	-- logger.warn('struct_cdata', p[0], ctype_id)
	local unpacker = common.msgpack_unpacker[ctype_id]
	-- seek to start offset of actual payload
	rb:seek_from_curr(4 + 1)
	len = len - 4 - 1
	if unpacker then
		return unpacker(rb, len)
	else
		local ct, ctp = common.ctype_from_id(ctype_id)
		local ptr
		if len == ffi.sizeof(ct) then
			ptr = ffi.new(ct)
		else
			ptr = ffi.new(ctp, len / ffi.sizeof(ct))
		end
		ffi.copy(ptr, rb:curr_byte_p(), len)
		rb:seek_from_curr(len)
		return ptr
	end
end
function serde_mt.unpack_ext_error(rb, size)
	local i = 0
	local name, bt, args
	local p = serde_mt.wait_data_arrived(rb, 2 + size)
	local clen = serde_mt.conv:ptr2unsigned(p + 2, size)
	rb:seek_from_curr(2 + size)
	serde_mt.wait_data_arrived(rb, clen)
	-- unpack error object
	name = serde_mt.unpack_any(rb) 
	bt = serde_mt.unpack_any(rb) 
	args = serde_mt.unpack_any(rb)
	return exception.new_with_bt(name, bt, unpack(args))
end
function serde_mt.unpack_ext_function(rb, size)
	local p = serde_mt.wait_data_arrived(rb, 1 + size)
	-- [type(1byte)][len(size byte)][payload]
	-- payload = [ext_type(1byte)][function body]
	local clen = serde_mt.conv:ptr2unsigned(p + 1, size)
	-- +1 and clen - 1 for skipping ext_type
	rb:seek_from_curr(1 + size)
	p = serde_mt.wait_data_arrived(rb, clen)
	local code = ffi.string(p + 1, clen - 1)
	local fn = loadstring(code)
	rb:seek_from_curr(clen)
	return fn
end
function serde_mt.unpack_ext(rb, size)
	local p = serde_mt.wait_data_arrived(rb, size)
	local t = p[1 + size]
	--logger.warn('unpack_ext', t)
	if t == EXT_CDATA_TYPE then
		return serde_mt.unpack_ext_struct_cdata(rb, size)
	elseif t == EXT_ERROR then
		return serde_mt.unpack_ext_error(rb, size)
	elseif t == EXT_FUNCTION then
		return serde_mt.unpack_ext_function(rb, size)
	else
		exception.raise('invalid', 'currently, varext represent struct cdata/error/function', t)
	end
end
function serde_mt.unpack_ext_numeric_cdata(rb, size)
	local p = serde_mt.wait_data_arrived(rb, 3 + size)
	rb:seek_from_curr(3 + size)
	if p[1] == EXT_CDATA_INT then
		if p[2] ~= 0 then
			return serde_mt.conv:ptr2unsigned(p + 3, size)
		else
			return serde_mt.conv:ptr2signed(p + 3, size)
		end
	elseif p[1] == EXT_CDATA_FLOAT then
		if p[2] ~= 0 then
			return serde_mt.conv:ptr2float(p + 3, size)
		else
			return serde_mt.conv:ptr2float(p + 3, size)
		end
	else
		exception.raise('invalid', 'currently, fixext always represent numeric cdata', p[1])
	end		
end

function serde_mt.unpack_int(rb, size)
	local ofs = 1 + size
	local p = serde_mt.wait_data_arrived(rb, ofs)
	rb:seek_from_curr(ofs)
	return serde_mt.conv:ptr2signed(p + 1, size)
end
function serde_mt.unpack_uint(rb, size)
	local ofs = 1 + size
	local p = serde_mt.wait_data_arrived(rb, ofs)
	rb:seek_from_curr(ofs)
	return serde_mt.conv:ptr2unsigned(p + 1, size)
end
function serde_mt.unpack_bin(rb, size)
	local ofs = 1 + size
	local p = serde_mt.wait_data_arrived(rb, ofs)
	local len = serde_mt.conv:ptr2unsigned(p + 1, size)
	rb:seek_from_curr(ofs)
	p = serde_mt.wait_data_arrived(rb, len)
	local str = ffi.string(p, len)
	rb:seek_from_curr(len)
	return str
end

function serde_mt.unpack_fixint(rb)
	local c = rb:curr_byte_p()[0]
	rb:seek_from_curr(1)
	return tonumber(c)
end
function serde_mt.unpack_negative_fixint(rb)
	local b = rb:curr_p()[0]
	rb:seek_from_curr(1)
	return tonumber(b)
end


function serde_mt.unpack_fixmap(rb)
	local size = tonumber(rb:curr_byte_p()[0]) - 0x80
	rb:seek_from_curr(1)
	return serde_mt.unpack_map(rb, size)
end
function serde_mt.unpack_fixarray(rb)
	local size = tonumber(rb:curr_byte_p()[0]) - 0x90
	rb:seek_from_curr(1)
	return serde_mt.unpack_array(rb, size)
end
function serde_mt.unpack_fixstr(rb)
	local size = tonumber(rb:curr_byte_p()[0]) - 0xa0
	rb:seek_from_curr(1)
	local p = serde_mt.wait_data_arrived(rb, size)
	local str = ffi.string(p, size)
	rb:seek_from_curr(size)
	return str
end

function serde_mt.unpack_nil(rb)
	rb:seek_from_curr(1)
	return nil
end
function serde_mt.unpack_true(rb)
	rb:seek_from_curr(1)
	return true
end
function serde_mt.unpack_false(rb)
	rb:seek_from_curr(1)
	return false
end

function serde_mt.unpack_bin8(rb)
	return serde_mt.unpack_bin(rb, 1)
end
function serde_mt.unpack_bin16(rb)
	return serde_mt.unpack_bin(rb, 2)
end
function serde_mt.unpack_bin32(rb)
	return serde_mt.unpack_bin(rb, 4)
end

function serde_mt.unpack_ext8(rb)
	return serde_mt.unpack_ext(rb, 1)
end
function serde_mt.unpack_ext16(rb)
	return serde_mt.unpack_ext(rb, 2)
end
function serde_mt.unpack_ext32(rb)
	return serde_mt.unpack_ext(rb, 4)
end

function serde_mt.unpack_fixext8(rb)
	return serde_mt.unpack_ext_numeric_cdata(rb, 1)
end
function serde_mt.unpack_fixext16(rb)
	return serde_mt.unpack_ext_numeric_cdata(rb, 2)
end
function serde_mt.unpack_fixext32(rb)
	return serde_mt.unpack_ext_numeric_cdata(rb, 4)
end
function serde_mt.unpack_fixext64(rb)
	return serde_mt.unpack_ext_numeric_cdata(rb, 8)
end
function serde_mt.unpack_fixext128(rb)
	return serde_mt.unpack_ext_numeric_cdata(rb, 16)
end

function serde_mt.unpack_float32(rb)
	local p = serde_mt.wait_data_arrived(rb, 5)
	local n = serde_mt.conv:ptr2float(p + 1, 4)
	rb:seek_from_curr(5)
	return n
end
function serde_mt.unpack_float64(rb)
	local p = serde_mt.wait_data_arrived(rb, 9)
	local n = serde_mt.conv:ptr2float(p + 1, 8)
	rb:seek_from_curr(9)
	return n
end

function serde_mt.unpack_uint8(rb)
	return serde_mt.unpack_uint(rb, 1)
end
function serde_mt.unpack_uint16(rb)
	return serde_mt.unpack_uint(rb, 2)
end
function serde_mt.unpack_uint32(rb)
	return serde_mt.unpack_uint(rb, 4)
end
function serde_mt.unpack_uint64(rb)
	return serde_mt.unpack_uint(rb, 8)
end

function serde_mt.unpack_int8(rb)
	return serde_mt.unpack_int(rb, 1)
end
function serde_mt.unpack_int16(rb)
	return serde_mt.unpack_int(rb, 2)
end
function serde_mt.unpack_int32(rb)
	return serde_mt.unpack_int(rb, 4)
end
function serde_mt.unpack_int64(rb)
	return serde_mt.unpack_int(rb, 8)
end

function serde_mt.unpack_str8(rb)
	return serde_mt.unpack_bin(rb, 1)
end
function serde_mt.unpack_str16(rb)
	return serde_mt.unpack_bin(rb, 2)
end
function serde_mt.unpack_str32(rb)
	return serde_mt.unpack_bin(rb, 4)
end

function serde_mt.unpack_array16(rb)
	local p = serde_mt.wait_data_arrived(rb, 3)
	local len = serde_mt.conv:ptr2unsigned(p + 1, 2)
	rb:seek_from_curr(3)
	return serde_mt.unpack_array(rb, len)
end
function serde_mt.unpack_array32(rb)
	local p = serde_mt.wait_data_arrived(rb, 5)
	local len = serde_mt.conv:ptr2unsigned(p + 1, 4)
	rb:seek_from_curr(5)
	return serde_mt.unpack_array(rb, len)
end

function serde_mt.unpack_map16(rb)
	local p = serde_mt.wait_data_arrived(rb, 3)
	local len = serde_mt.conv:ptr2unsigned(p + 1, 2)
	rb:seek_from_curr(3)
	return serde_mt.unpack_map(rb, len)
end
function serde_mt.unpack_map32(rb)
	local p = serde_mt.wait_data_arrived(rb, 5)
	local len = serde_mt.conv:ptr2unsigned(p + 1, 4)
	rb:seek_from_curr(5)
	return serde_mt.unpack_map(rb, len)
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

function serde_mt.unpack_any(rb)
	serde_mt.wait_data_arrived(rb, 1)
	local t = tonumber(rb:curr_byte_p()[0])
	-- logger.info('unpack_any', rb, t, tostring(serde_mt.unpacker[t]), rb.hpos, rb.used, rb:available())
	return serde_mt.unpacker[t](rb)
end

function serde_mt.start_unpacker(rb)
	-- logger.warn('start_unpacker', tostring(rb))
	while true do
		-- logger.warn('start_unpacker', rb:available())
		coroutine.yield(true, serde_mt.unpack_any(rb))
	end
end
function serde_mt:stream_unpacker(rb)
	local co = coroutine.create(serde_mt.start_unpacker)
	coroutine.resume(co, rb)
	return co
end
function serde_mt:unpack_packet(ctx)
	-- logger.warn('unpack_packet', ctx)
	local ok, fin, r = coroutine.resume(ctx)
	if ok then
		-- logger.warn('unpack_packet:', tostring(ok), tostring(fin), tostring(r))
		return r, nil, fin
	else
		return nil, fin
	end
end
function serde_mt:unpack(rb)
	return serde_mt.unpack_any(rb)
end
ffi.metatype('luact_msgpack_serde_t', serde_mt)

return memory.alloc_typed('luact_msgpack_serde_t')





