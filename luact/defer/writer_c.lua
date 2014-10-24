local ffi = require 'ffiex.init'

local pulpo = require 'pulpo.init'
local thread = require 'pulpo.thread'
local socket = require 'pulpo.socket'

local _M = (require 'pulpo.package').module('luact.defer.writer_c')

--[[
	write commands
--]]
local WRITER_NONE = 0
local WRITER_RAW = 1
local WRITER_VEC = 2
_M.WRITER_NONE = WRITER_NONE

--[[
	cdef
--]]
ffi.cdef([[
	typedef struct luact_writer_raw {
		size_t sz, ofs;
		char p[0];
	} luact_writer_raw_t;
	typedef struct luact_writer_vec {
		size_t sz, ofs;
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
function writer_raw_index.required_size(p, sz)
	return sz + (2 * size_t_size)
end
function writer_raw_index.write(wb, p, sz)
	local append = wb:reserve_with_cmd(
		writer_raw_index.required_size(p, sz), WRITER_RAW
	)
	local pv = ffi.cast('luact_writer_raw_t', wb.last_p)
	if append then
		ffi.copy(pv.p + p.sz, p, sz)
		pv.sz = pv.sz + sz
		return pv.sz
	else 
		ffi.copy(pv.p, p, sz)
		pv.ofs = 0
		pv.sz = sz
		return sz
	end
end
-- after copied into write buff
function writer_raw_index:syscall(io)
	return io:write(self.p + self.ofs, self.sz - self.ofs)
end
function writer_raw_index:sent(r)
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
function writer_vec_index.required_size(iov, sz)
	return (sz * iovec_size) + 1 + (2 * size_t_size)
end
function writer_vec_index.write(wb, p, sz)
	local append = wb:reserve_with_cmd(
		writer_vec_index.required_size(p, sz), WRITER_VEC
	)
	local pv = ffi.cast('luact_writer_vec_t', wb.last_p)
	if append then
		ffi.copy(pv.p + p.sz, p, sz * iovec_size)
		pv.sz = pv.sz + sz
		return pv.sz
	else 
		ffi.copy(pv.p, p, sz)
		pv.ofs = 0
		pv.sz = sz
		return sz
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

function writer_serde_index.writer(wb, sr, obj)
	local append = wb:reserve_with_cmd(0, WRITER_RAW)
	local pv = ffi.cast('luact_writer_raw_t', wb.last_p)
	if append then
		pv.sz = pv.sz + sr:pack(obj, pv.p, wb)
	else 
		pv.ofs = 0
		pv.sz = sr:pack(obj, wb)
	end	
	return pv.sz
end
_M.serde = writer_serde_index

_M[WRITER_RAW] = ffi.typeof('luact_writer_raw_t')
_M[WRITER_VEC] = ffi.typeof('luact_writer_vec_t')

end) -- thread.add_initializer

return _M
