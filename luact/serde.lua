local ffi = require 'ffiex.init'
local reflect = require 'reflect'
local socket = require 'pulpo.socket'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local writer = require 'luact.writer'

local _M = {}
local kind = {
	serpent = 1,
	json = 2,
	msgpack = 3,
	protobuf = 4,
}
_M.kind = kind

local WRITER_RAW = writer.WRITER_RAW



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
local custom_pack = {}
local custom_unpack = {}
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
		tmp[idx] = ffi.string(arg, refl.size)
		return {name = 'array', tp = refl.element_type.name}
	elseif refl.what == 'ptr' or refl.what == 'ref' then
		local et = refl.element_type
		local name = et.what.." "..et.name
		if custom_pack[name] then
			tmp[idx] = custom_pack[name](arg)
		else
			tmp[idx] = ffi.string(arg, refl.element_type.size)
		end
		-- print('refl.name = ', refl.name, 'size = ', refl.element_type.size)
		return {name = refl.what, tp = name}
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
ffi.metatype('bytearray_scalar_conv_t', b2s_conv_mt)
local conv = ffi.new('bytearray_scalar_conv_t')


-- serpent
local sp = require 'serpent'
local serpent = {}

local depth = 0
function serpent.serializer(t)
	depth = depth + 1
	local esc = ("="):rep(depth)
	local ok, r = pcall(string.format, 
		"(require 'pulpo.exception').unserialize([[%s]],[[\n%s]],[%s[%s]%s])", 
		t.name, t.bt, esc, sp.dump(t.args), esc)
	depth = depth - 1
	return ok and r or tostring(t)
end

_M[kind.serpent] = serpent
function serpent:pack_packet(buf, append, ...)
	if _M.DEBUG then
		logger.notice('packets', ...)
	end
	exception.serializer = serpent.serializer
	local str = sp.dump(conv:escape({...}))
	local data = tostring(#str)..":"..str
	local sz, pv = #data, nil
	if append then
		buf:reserve(sz)
		-- pv must get after necessary size allocated. 
		-- because reserve changed internal pointer of buf
		pv = ffi.cast('luact_writer_raw_t*', buf:curr_p())
		ffi.copy(pv.p + pv.sz, data, sz)
		pv.sz = pv.sz + sz
		buf:use(sz)
	else 
		buf:reserve_with_cmd(sz, WRITER_RAW)
		pv = ffi.cast('luact_writer_raw_t*', buf:curr_p())
		pv.sz = sz
		pv.ofs = 0
		ffi.copy(pv.p, data, sz)
		buf:use(ffi.sizeof('luact_writer_raw_t') + sz)
	end	
	if _M.DEBUG then
		logger.warn('packed:', data)
	end
	return sz
end
function serpent:unpack_packet(rb)
	local sz, len, p = 0, rb:available(), rb:curr_p()
	if _M.DEBUG then
		print('unpack:', len,ffi.string(p, len))
	end
	while sz < len do
		if p[sz] ~= (":"):byte() then
			sz = sz + 1
		else
			break
		end
	end	
	if sz <= 0 then
		-- not enough buffer. keep on reading buffer	
		return nil
	end
	local ok, dsz = pcall(tonumber, ffi.string(p, sz))
	if not ok then
		return nil, exception.new('invalid', 'format', ffi.string(p))
	end
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
			rb:seek_from_curr(sz + 1 + dsz)
			local r = {pcall(conv.unescape, conv, fn())}
			if not r[1] then
				-- logger.report('err unescape:', ffi.string(p + sz + 1, dsz))
				error(r[2])
			end
			return unpack(r, 2)
		end
	end
end
function serpent:pack(buf, obj)
	if _M.DEBUG then
		logger.notice('packer', obj)
	end
	exception.serializer = serpent.serializer
	local str = sp.dump(conv:escape(obj))
	local data = tostring(#str)..":"..str
	local sz, pv = #data, nil
	buf:reserve(sz)
	ffi.copy(buf:curr_p(), data, sz)
	buf:use(sz)
	if _M.DEBUG then
		logger.notice('packed:', data, #data)
	end
	return sz
end
serpent.unpack = serpent.unpack_packet
function serpent:customize(ctype, pack, unpack)
	custom_pack[ctype] = pack
	custom_unpack[ctype] = unpack
end

-- json
local dkjson = require 'dkjson'
local json = {}
_M[kind.json] = json
function json:pack_packet(buf, append, ...)
end
function json:unpack_packet(rb)
end
function json:pack(buf, ...)
end
function json:unpack(rb)
end

-- msgpack
local mpk = require 'msgpack'
local msgpack = {}
_M[kind.msgpack] = msgpack
function msgpack:pack_packet(buf, append, ...)
	-- TODO : it is probably very costly to expand buffer on-demand, 
	-- (espacially because msgpack packer writes data to buf ptr directly)
end
function msgpack:unpack_packet(rb)
end
function msgpack:pack(buf, ...)
	-- TODO : it is probably very costly to expand buffer on-demand, 
	-- (espacially because msgpack packer writes data to buf ptr directly)
end
function msgpack:unpack(rb)
end

-- protobuf (TODO)
local protobuf = {}
_M[kind.protobuf] = protobuf
function protobuf:pack_packet(buf, append, ...)
end
function protobuf:unpack_packet(rb)
end
function protobuf:pack(buf, ...)
end
function protobuf:unpack(rb)
end

return _M
