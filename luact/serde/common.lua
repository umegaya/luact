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

function b2s_conv_index:unsigned2ptr(v, rfl)
	if rfl.size == 1 then
		self.b = v
	elseif rfl.size == 2 then
		self.us = v
	elseif rfl.size == 4 then
		self.u = v
	elseif rfl.size == 8 then
		self.ull = v
	end
	return self.p
end
function b2s_conv_index:ptr2unsigned(ptr)
	local size = #ptr
	ffi.copy(self.p, ptr, size)
	if size == 1 then
		return self.b
	elseif size == 2 then
		return self.us
	elseif size == 4 then
		return self.u
	elseif size == 8 then
		return self.ull
	end
end
function b2s_conv_index:signed2ptr(v, rfl)
	if rfl.size == 1 then
		self.c = v
	elseif rfl.size == 2 then
		self.s = v
	elseif rfl.size == 4 then
		self.i = v
	elseif rfl.size == 8 then
		self.ll = v
	end
	return self.p
end
function b2s_conv_index:ptr2signed(ptr, size)
	local size = size or #ptr
	ffi.copy(self.p, ptr, size)
	if size == 1 then
		return self.c
	elseif size == 2 then
		return self.s
	elseif size == 4 then
		return self.i
	elseif size == 8 then
		return self.ll
	end
end
function b2s_conv_index:float2ptr(v, rfl)
	if rfl.size == 4 then
		self.f = v
	elseif rfl.size == 8 then
		self.d = v
	end
	return self.p
end
function b2s_conv_index:ptr2float(ptr, size)
	local size = size or #ptr
	ffi.copy(self.p, ptr, size)
	if size == 4 then
		return self.f
	elseif size == 8 then
		return self.d
	end
end
local custom_pack = {}
local custom_unpack = {}
_M.custom_pack = custom_pack
_M.custom_unpack = custom_unpack
function b2s_conv_index:escape_cdata(tmp, idx, arg)
	local refl = reflect.typeof(arg)
-- print('cdata arg:', refl.what, refl.name, tostring(arg))
	if refl.what == 'int' or refl.what == 'enum' then
		if refl.unsigned then
			tmp[idx] = ffi.string(self:unsigned2ptr(arg, refl), refl.size)
		else
			tmp[idx] = ffi.string(self:signed2ptr(arg, refl), refl.size)
		end
		return {name = 'int', unsigned = refl.unsigned}
	elseif refl.what == 'float' then
		tmp[idx] = ffi.string(self:float2ptr(arg, refl), refl.size)
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