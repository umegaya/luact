local ffi = require 'ffiex.init'
local reflect = require 'reflect'
local socket = require 'pulpo.socket'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'

_M = {}

-- helper for packing some kind of cdata (int, float, enum, constant)
ffi.cdef [[
	typedef union luact_bytearray_scalar_conv {
		float f;
		double d;
		uint8_t b;
		int8_t c;
		uint16_t us;
		int16_t s;
		uint32_t u;
		int32_t i;
		uint64_t ull;
		int64_t ll;
		char p[0];
	} luact_bytearray_scalar_conv_t;
]]

local b2s_conv_index = {}
local b2s_conv_mt = {
	__index = b2s_conv_index
}

function b2s_conv_index:unsigned2ptr(v, size)
	if size == 1 then
		self.b = v
	elseif size == 2 then
		self.us = socket.htons(v)
	elseif size == 4 then
		self.u = socket.htonl(v)
	elseif size == 8 then
		self.ull = socket.htonll(v)
	end
	return self.p
end
function b2s_conv_index:ptr2unsigned(ptr, size)
	size = size or #ptr
	ffi.copy(self.p, ptr, size)
	if size == 1 then
		return self.b
	elseif size == 2 then
		return socket.ntohs(self.us)
	elseif size == 4 then
		return socket.ntohl(self.u)
	elseif size == 8 then
		return socket.ntohll(self.ull)
	end
end
function b2s_conv_index:signed2ptr(v, size)
	if size == 1 then
		self.c = v
	elseif size == 2 then
		self.s = socket.htons(v)
	elseif size == 4 then
		self.i = socket.htonl(v)
	elseif size == 8 then
		self.ll = socket.htonll(v)
	end
	return self.p
end
function b2s_conv_index:ptr2signed(ptr, size)
	local size = size or #ptr
	ffi.copy(self.p, ptr, size)
	if size == 1 then
		return self.c
	elseif size == 2 then
		return socket.ntohs(self.s)
	elseif size == 4 then
		return socket.ntohl(self.i)
	elseif size == 8 then
		return socket.ntohll_signed(self.ll)
	end
end
function b2s_conv_index:float2ptr(v, size)
	if size == 4 then
		self.f = v
		self.u = socket.htonl(self.u)
	elseif size == 8 then
		self.d = v
		self.ull = socket.htonll(self.ull)
	end
	return self.p
end
function b2s_conv_index:ptr2float(ptr, size)
	local size = size or #ptr
	ffi.copy(self.p, ptr, size)
	if size == 4 then
		self.u = socket.ntohl(self.u)
		return self.f
	elseif size == 8 then
		self.ull = socket.ntohll(self.ull)
		return self.d
	end
end

local map_ctype_id = {
	struct = {},
	union = {},
}
local map_id_ctype = {}
local map_id_ctype_ptr = {}
-- reserve system ctype id
_M.LUACT_UUID = 1
_M.LUACT_GOSSIP_NODELIST = 2
_M.LUACT_RAFT_SNAPSHOT_HEADER = 3
_M.LUACT_BUF_SLICE = 4
_M.LUACT_DHT_RANGE = 5
_M.LUACT_DHT_KEY = 6
_M.LUACT_DHT_CMD_GET = 7
_M.LUACT_DHT_CMD_PUT = 8
_M.LUACT_DHT_CMD_CAS = 9
_M.LUACT_DHT_CMD_MERGE = 10
_M.LUACT_DHT_CMD_WATCH = 11
_M.LUACT_DHT_CMD_SPLIT = 12
_M.LUACT_DHT_GOSSIP_REPLICA_CHANGE = 13
_M.LUACT_DHT_GOSSIP_RANGE_SPLIT = 14
_M.LUACT_RAFT_HARDSTATE = 15
_M.LUACT_DHT_CMD_SCAN = 16

-- packer maps
_M.serpent_packer = {}
_M.json_packer = {}
_M.protobuf_packer = {}
_M.msgpack_packer = {}
-- unpacker maps
_M.serpent_unpacker = {}
_M.json_unpacker = {}
_M.protobuf_unpacker = {}
_M.msgpack_unpacker = {}
function _M.register_ctype(what, name, serde, id)
	-- logger.info('register_ctype', what, name, tostring(serde), id)
	local t = what.." "..name
	if id then
		map_ctype_id[what][name] = id
		map_id_ctype[id] = ffi.typeof(t)
		map_id_ctype_ptr[id] = ffi.typeof("$[?]", ffi.typeof(t))
	else
		-- TODO : generate id autometically by registering it to dht
		local defs = ffi.src_of(t)
	end
	if serde then
		for kind, procs in pairs(serde) do
			local packer_key = kind.."_packer"
			local unpacker_key = kind.."_unpacker"
			_M[packer_key][id] = procs.packer
			_M[unpacker_key][id] = procs.unpacker
		end
	end
end
function _M.ctype_id(what, name)
	return map_ctype_id[what][name]
end
function _M.ctype_from_id(id)
	local tmp = tonumber(id)
	-- logger.warn(id, map_id_ctype[tmp], map_id_ctype_ptr[tmp])
	return map_id_ctype[tmp], map_id_ctype_ptr[tmp]
end


-- TODO : move below to serpent and also using ctype id
local custom_pack = {}
local custom_unpack = {}
_M.custom_pack = custom_pack
_M.custom_unpack = custom_unpack
function b2s_conv_index:escape_cdata(tmp, idx, arg)
	local refl = reflect.typeof(arg)
-- print('cdata arg:', refl.what, refl.name, tostring(arg))
	if refl.what == 'int' or refl.what == 'enum' then
		if refl.unsigned then
			tmp[idx] = ffi.string(self:unsigned2ptr(arg, refl.size), refl.size)
		else
			tmp[idx] = ffi.string(self:signed2ptr(arg, refl.size), refl.size)
		end
		return {name = 'int', unsigned = refl.unsigned}
	elseif refl.what == 'float' then
		tmp[idx] = ffi.string(self:float2ptr(arg, refl.size), refl.size)
		return {name = 'float'}
	elseif refl.what == 'struct' or refl.what == 'union' then
		-- print('refl.name = ', refl.name, 'size = ', refl.size)
		local name = refl.what.." "..refl.name
		if custom_pack[name] then
			tmp[idx] = custom_pack[name](arg)
		else
			tmp[idx] = ffi.string(arg, refl.size)
		end
		return {name = name}
	elseif refl.what == 'array' then
		tmp[idx] = ffi.string(arg, refl.size == 'none' and ffi.sizeof(arg) or refl.size)
		return {name = 'array', tp = refl.element_type.name}
	elseif refl.what == 'ptr' or refl.what == 'ref' then
		local et = refl.element_type
		if et.name then -- struct/union
			local name = et.what.." "..et.name
			if custom_pack[name] then
				tmp[idx] = custom_pack[name](arg)
			else
				tmp[idx] = ffi.string(arg, et.size)
			end
			-- print('refl.name = ', refl.name, 'size = ', refl.element_type.size)
			return {name = refl.what, tp = name}
		else
			-- primitive type : CAUTION, ptr/ref is always treat as length 1
			tmp[idx] = ffi.string(arg, et.size)
		end
	end
end
function b2s_conv_index:escape(obj, no_root)
	local changed 
	local le = ((not no_root) and socket.little_endian() or nil)
	if type(obj) ~= 'table' then
		local tmp = { __le__ = le }
		if type(obj) == "cdata" then
			changed = true
			tmp.__cdata__ = self:escape_cdata(tmp, 1, obj)
		end
		return changed and tmp or obj
	else
		local tmp = { __cdatas__ = {}, __le__ = le }
		for idx,arg in pairs(obj) do
			if type(arg) == "cdata" then
				changed = true
				tmp.__cdatas__[idx] = self:escape_cdata(tmp, idx, arg)
			elseif type(arg) == 'table' and (not (getmetatable(arg) and getmetatable(arg).__serialize)) then
				changed = true
				tmp[idx] = self:escape(arg, true)
			else
				-- such an complex structure should pass as uuid to remote. 
				tmp[idx] = arg
			end
		end
		return changed and tmp or obj
	end
end
function b2s_conv_index:unescape_cdata(src, tp)
	if _M.DEBUG then
		print('unescape_cdata---------------------------------')
		for k,v in pairs(tp) do
			print('unescape_cdata', k, v)
		end
	end
	if tp.name == 'int' then
		if tp.unsigned then
			return self:ptr2unsigned(src)
		else
			return self:ptr2signed(src)
		end
	elseif tp.name == 'float' then
		return self:ptr2float(src)
	elseif tp.name == 'array' or tp.name == 'ptr' or tp.name == 'ref' then
		if (tp.name == 'ptr' or tp.name == 'ref') and custom_unpack[tp.tp] then
			return custom_unpack[tp.tp](src)
		elseif tp.name == 'ptr' or tp.name == 'array' then
			local tmp = memory.alloc_typed(tp.tp, #(src) / ffi.sizeof(tp.tp))
			ffi.copy(tmp, src, #src)
			return tmp
		else
			local tmp = memory.alloc_typed(tp.tp)
			ffi.copy(tmp, src, #src)
			return tmp[0]
		end
	elseif tp.name then
		if custom_unpack[tp.name] then
			return custom_unpack[tp.name](src)
		else
			local tmp = memory.alloc_typed(tp.name)
			ffi.copy(tmp, src, ffi.sizeof(tp.name))
			return tmp[0]
		end
	end
end
function b2s_conv_index:unescape(obj)
	-- TODO : check obj.le and if endian is not match with this node, do something like ntohs/ntohl/ntohll
	if obj.__cdata__ then
		return self:unescape_cdata(obj[1], obj.__cdata__)
	end
	if obj.__cdatas__ then
		for idx,tp in pairs(obj.__cdatas__) do
			obj[idx] = self:unescape_cdata(obj[idx], tp)
		end
		obj.__cdatas__ = nil
	end
	for k,v in pairs(obj) do
		if type(v) == 'table' and (v.__cdata__ or v.__cdatas__) then
			obj[k] = self:unescape(v)
		end
	end
	return obj
end
ffi.metatype('luact_bytearray_scalar_conv_t', b2s_conv_mt)

return _M
