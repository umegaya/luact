local common = require 'luact.serde.common'
local exception = require 'pulpo.exception'
local writer = require 'luact.writer'
local sp = require 'serpent'

local WRITER_RAW = writer.WRITER_RAW

-- cdefs
local conv = ffi.new('luact_bytearray_scalar_conv_t')


-- serpent
local _M = {}

local depth = 0
function _M.serializer(t)
	depth = depth + 1
	local esc = ("="):rep(depth)
	local ok, r = pcall(string.format, 
		"(require 'pulpo.exception').unserialize([[%s]],[[\n%s]],[%s[%s]%s])", 
		t.name, t.bt, esc, sp.dump(t.args), esc)
	depth = depth - 1
	return ok and r or tostring(t)
end
function _M:pack_packet(buf, append, ...)
	if _M.DEBUG then
		logger.notice('packets', ...)
	end
	exception.serializer = _M.serializer
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
function _M:pack(buf, obj)
	if _M.DEBUG then
		logger.notice('packer', obj)
	end
	exception.serializer = _M.serializer
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
function _M:unpack_packet(rb)
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
function _M:stream_unpacker(rb)
	return rb
end
_M.unpack = _M.unpack_packet
function _M:customize(ctype, pack, unpack)
	common.custom_pack[ctype] = pack
	common.custom_unpack[ctype] = unpack
end

return _M

