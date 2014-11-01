local ffi = require 'ffiex.init'
local reflect = require 'reflect'
local socket = require 'pulpo.socket'

local _M = {}
local kind = {
	serpent = 1,
	json = 2,
	msgpack = 3,
	protobuf = 4,
}
_M.kind = kind



-- cdefs
-- helper for packing some kind of cdata (int, float, enum, constant)
ffi.cdef [[
	typedef union bytearray_scalar_conv {
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
	} bytearray_scalar_conv_t;
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
function b2s_conv_index:ptr2signed(ptr)
	local size = #ptr
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
function b2s_conv_index:ptr2float(ptr)
	local size = #ptr
	ffi.copy(self.p, ptr, size)
	if size == 4 then
		return self.f
	elseif size == 8 then
		return self.d
	end
end
function b2s_conv_index:escape(...)
	local tmp = { cdata = {}, le = socket.little_endian() }
	for idx,arg in ipairs({...}) do
		if type(arg) == "cdata" then
			local refl = reflect.typeof(arg)
			if refl.what == 'int' or refl.what == 'enum' then
				if refl.unsigned then
					table.insert(tmp, ffi.string(self:unsigned2ptr(arg, refl), refl.size))
				else
					table.insert(tmp, ffi.string(self:signed2ptr(arg, refl), refl.size))	
				end
				tmp.cdata[idx] = {name = 'int', unsigned = refl.unsigned}
			elseif refl.what == 'float' then
				table.insert(tmp, ffi.string(self:float2ptr(arg, refl), refl.size))
				tmp.cdata[idx] = {name = refl.what}
			elseif refl.what == 'struct' then
				table.insert(tmp, ffi.string(arg, refl.size))
				tmp.cdata[idx] = {name = refl.name}
			elseif refl.what == 'array' then
				table.insert(tmp, ffi.string(arg, refl.size))
				tmp.cdata[idx] = {name = 'array', tp = refl.element_type.name}
			end
		else
			table.insert(tmp, arg)
		end
	end
	return tmp
end
function b2s_conv_index:unescape(obj)
	for idx,tp in pairs(obj.cdata) do
		if tp.name == 'int' then
			if tp.unsigned then
				obj[idx] = self:ptr2unsigned(obj[idx])
			else
				obj[idx] = self:ptr2signed(obj[idx])
			end
		elseif tp.name == 'float' then
			obj[idx] = self:ptr2float(obj[idx])
		elseif tp.name == 'array' then
			local tmp = memory.alloc_typed(tp.tp, #(obj[idx]) / ffi.sizeof(tp.tp))
			ffi.copy(tmp, obj[idx], #obj[idx])
			obj[idx] = tmp
		elseif tp.name then
			local tmp = memory.alloc_typed(tp.name)
			ffi.copy(tmp, obj[idx], ffi.sizeof(tp.name))
			obj[idx] = tmp
		end
	end
	return obj
end
ffi.metatype('bytearray_scalar_conv_t', b2s_conv_mt)
local conv = ffi.new('bytearray_scalar_conv_t')


-- serpent
local sp = require 'serpent'
local serpent = {}
_M[kind.serpent] = serpent
function serpent:pack(ptr, rb, ...)
	logger.notice('packer', ...)
	local str = sp.dump(conv:escape(...))
	local data = tostring(#str)..":"..str
	rb:reserve(#data)
	ffi.copy(ptr, data, #data)
	if true or _M.debug then
		print('packed:', data)
	end
	return #data
end
function serpent:unpack(rb)
	local p = rb:curr_p()
	local sz = 0
	while true do
		if p[sz] ~= (":"):byte() then
			sz = sz + 1
		else
			break
		end
	end	
	local dsz = tonumber(ffi.string(p, sz))
	if rb:available() < (dsz + 1 + sz) then
		-- not enough buffer. keep on reading buffer
		return nil
	else
		local fn, err = loadstring(ffi.string(p + sz + 1, dsz))
		if err then
			-- fatal. invalid record
			return nil, err
		else
			-- can have valid record.
			rb:shrink(sz + 1 + dsz)
			return conv:unescape(fn())
		end
	end
end

-- json
local dkjson = require 'dkjson'
local json = {}
_M[kind.json] = json
function json:pack(ptr, rb, ...)
end
function json:unpack(rb)
end

-- msgpack
local mpk = require 'msgpack'
local msgpack = {}
_M[kind.msgpack] = msgpack
function msgpack:pack(ptr, rb, ...)
end
function msgpack:unpack(rb)
end

-- protobuf (TODO)
local protobuf = {}
_M[kind.protobuf] = protobuf
function protobuf:pack(ptr, rb, ...)
end
function protobuf:unpack(rb)
end

return _M
