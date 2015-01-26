local ffi = require 'ffiex.init'

local pulpo = require 'pulpo.init'
local thread = require 'pulpo.thread'
local socket = require 'pulpo.socket'

local _M = (require 'pulpo.package').module('luact.defer.writer_c')

--[[
	write commands
--]]
local WRITER_NONE = _M.WRITER_NONE
local WRITER_RAW = _M.WRITER_RAW
local WRITER_VEC = _M.WRITER_VEC


--[[
	cdef
--]]
ffi.cdef([[
	typedef struct luact_writer_raw {
		luact_bufsize_t sz, ofs;
		char p[0];
	} luact_writer_raw_t;
	typedef struct luact_writer_vec {
		luact_bufsize_t sz, ofs;
		struct iovec p[0];
	} luact_writer_vec_t;
]])



--[[
 	wbuf writer:raw
--]]
local writer_raw_index = {}
local writer_raw_mt = { __index = writer_raw_index }
local size_t_size = ffi.sizeof('size_t')

-- before copied into write buff
writer_raw_index.cmd = WRITER_RAW
function writer_raw_index.required_size(sz)
	return sz + (2 * size_t_size)
end
function writer_raw_index.write(buf, append, p, sz)
	local pv
	local reqsize = writer_raw_index.required_size(sz)
	if append then
		buf:reserve(reqsize)
		pv = ffi.cast('luact_writer_raw_t*', buf:curr_p())
		ffi.copy(pv.p + p.sz, p, sz)
		pv.sz = pv.sz + sz
		buf:use(sz)
	else 
		buf:reserve_with_cmd(reqsize, WRITER_RAW)
		pv = ffi.cast('luact_writer_raw_t*', buf:curr_p())
		ffi.copy(pv.p, p, sz)
		pv.ofs = 0
		pv.sz = sz
		buf:use(ffi.sizeof('luact_writer_raw_t') + sz)
	end
end
-- after copied into write buff
function writer_raw_index:syscall(_io)
	--logger.info('write', _io, self, self.ofs, self.sz, self.p)
	--for i=0,tonumber(self.sz)-1 do
	--	io.write(('%02x'):format(ffi.cast('uint8_t *', self.p)[i])..":")
	--end
	--io.write('\n')
	return _io:write(self.p + self.ofs, self.sz - self.ofs)
end
function writer_raw_index:sent(r)
	-- logger.info('sent:', r, 'bytes')
	self.ofs = self.ofs + r
end
function writer_raw_index:finish()
	return self.ofs >= self.sz
end
function writer_raw_index:chunk_size()
	return self.sz + 1 + (2 * size_t_size)
end
ffi.metatype('luact_writer_raw_t', writer_raw_mt)
_M.raw = writer_raw_index



--[[
 	wbuf writer:vec
--]]
local writer_vec_index = {}
local writer_vec_mt = { __index = writer_vec_index }
local iovec_size = ffi.sizeof('struct iovec')

-- before copied into write buff
writer_vec_index.cmd = WRITER_VEC
function writer_vec_index.required_size(sz)
	return (sz * iovec_size) + 1 + (2 * size_t_size)
end
function writer_vec_index.write(buf, append, p, sz)
	local pv
	local reqsize = writer_vec_index.required_size(sz)
	if append then
		buf:reserve(reqsize)
		pv = ffi.cast('luact_writer_vec_t*', buf:curr_p())
		ffi.copy(pv.p + p.sz, p, sz * iovec_size)
		pv.sz = pv.sz + sz
		buf:use(sz * iovec_size)
	else 
		buf:reserve_with_cmd(reqsize, WRITER_VEC)
		pv = ffi.cast('luact_writer_vec_t*', buf:curr_p())
		ffi.copy(pv.p, p, sz * iovec_size)
		pv.ofs = 0
		pv.sz = sz
		buf:use(ffi.sizeof('luact_writer_vec_t') + (sz * iovec_size))
	end
end
-- after copied into write buff
function writer_vec_index:syscall(io)
	return io:writev(self.p + self.ofs, self.sz)
end
function writer_vec_index:sent(sz)
	for i=self.ofs,self.sz,1 do
		if sz >= self.p[i].iov_len then
			sz = sz - self.p[i].iov_len
		else
			self.p[i].iov_base = self.p[i].iov_base + sz
			self.p[i].iov_len = self.p[i].iov_len - sz
			self.ofs = i
			break
		end
	end
end
function writer_raw_index:finish()
	return self.ofs >= self.sz
end
function writer_vec_index:chunk_size()
	return (self.sz * iovec_size) + 1 + (2 * size_t_size)
end
ffi.metatype('luact_writer_vec_t', writer_vec_mt)
_M.vec = writer_vec_index



--[[
 	wbuf writer:serde
--]]
local writer_serde_index = pulpo.util.copy_table(writer_raw_index)

function writer_serde_index.write(buf, append, sr, ...)
	sr:pack_packet(buf, append, ...)
end
_M.serde = writer_serde_index

_M[WRITER_RAW] = ffi.typeof('luact_writer_raw_t*')
_M[WRITER_VEC] = ffi.typeof('luact_writer_vec_t*')

return _M
