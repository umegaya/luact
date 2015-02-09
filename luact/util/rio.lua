local luact = require 'luact.init'
local serde = require 'luact.serde'
local serde_common = require 'luact.serde.common'
local ffi = require 'ffiex.init'
local fs = require 'pulpo.fs'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local util = require 'pulpo.util'

local C = ffi.C
local _M = {}

-- cdef
ffi.cdef [[
typedef struct luact_buf_slice {
	size_t sz;
	char p[0];
} luact_buf_slice_t;
typedef struct luact_remote_io {
	int fd;	
} luact_remote_io_t;
typedef struct luact_remote_mem {
	char *ptr;
	size_t sz;
	bool gc;
} luact_remote_mem_t;
]]

-- luact_buf_slice
local buf_slice_index = {}
local buf_slice_mt = {
	__index = buf_slice_index,
}
function buf_slice_index.size(sz)
	return ffi.sizeof('luact_buf_slice_t') + sz
end
function buf_slice_index.alloc(sz)
	local p = ffi.cast('luact_buf_slice_t*', memory.alloc(buf_slice_index.size(sz)))
	p.sz = sz
	return p
end
function buf_slice_index:fin()
	memory.free(self)
end
function buf_slice_index:reserve(sz)
	if sz > self.sz then
		local newsize = self.sz
		while sz > newsize do
			newsize = newsize * 2
		end
		local tmp = memory.realloc(self, buf_slice_index.size(newsize))
		if tmp == ffi.NULL then
			exception.raise('malloc', 'char', readbuf_size)
		end
		tmp.sz = newsize
		return tmp
	end
	return self
end
function buf_slice_index.pack(arg)
	return ffi.string(arg, buf_slice_index.size(arg.sz))
end
function buf_slice_index.unpack(arg)
	return ffi.cast('luact_buf_slice_t*', arg)
end
-- for serpent serializer
serde[serde.kind.serpent]:customize(
	'struct luact_buf_slice', 
	buf_slice_index.pack, buf_slice_index.unpack
)
serde_common.register_ctype('struct', 'luact_buf_slice', {
	msgpack = {
		packer = function (pack_procs, buf, ctype_id, obj, length)
			buf:reserve(obj.sz)
			local p, ofs = pack_procs.pack_ext_cdata_header(buf, obj.sz, ctype_id)
			ffi.copy(p + ofs, obj.p, obj.sz)
			return ofs + obj.sz
		end,
		unpacker = function (rb, len)
			local ptr = buf_slice_index.alloc(len)
			ffi.copy(ptr.p, rb:curr_p(), len)
			rb:seek_from_curr(len)
			return ffi.gc(ptr, memory.free)
		end,
	}, 
}, serde_common.LUACT_BUF_SLICE)
ffi.metatype('luact_buf_slice_t', buf_slice_mt)
local readbuf_size = 1024
local readbuf = buf_slice_index.alloc(readbuf_size)


-- luact_remote_io
local remote_io_index = {}
local remote_io_mt = {
	__index = remote_io_index
}
function remote_io_index:read(size, ofs)
	if ofs then fs.seek(self.fd, ofs, fs.SEEK_SET) end
	size = size or readbuf_size
	readbuf.sz = readbuf_size
	readbuf:reserve(size)
	if readbuf.sz ~= readbuf_size then
		readbuf_size = readbuf.sz
	end
	local ret = C.read(self.fd, readbuf.p, size)
	if ret < 0 then
		exception.raise('syscall', 'read', self.fd)
	elseif ret == 0 then
		return nil
	else
		readbuf.sz = ret
		return readbuf
	end
end
function remote_io_index:write(buf, size, ofs)
	if ofs then fs.seek(self.fd, ofs, fs.SEEK_SET) end
	return C.write(self.fd, buf, size)
end
function remote_io_index:seek(whence, ofs)
	fs.seek(self.fd, ofs, whence)
end
function remote_io_index:close()
	if self.fd >= 0 then
		C.close(self.fd)
		self.fd = -1
	end
end
function remote_io_index:__actor_destroy__()
	self:close()
	memory.free(self)
end
ffi.metatype('luact_remote_io_t', remote_io_mt)


-- luact_remote_io
local remote_mem_index = util.copy_table(remote_io_index)
local remote_mem_mt = {
	__index = remote_mem_index
}
function remote_mem_index:read(size, ofs)
	ofs = ofs or 0
	size = size or readbuf_size
	readbuf.sz = readbuf_size
	readbuf:reserve(size)
	if readbuf.sz ~= readbuf_size then
		readbuf_size = readbuf.sz
	end
	size = math.min(self.sz - ofs, size)
	ffi.copy(readbuf.p, self.ptr + ofs, size)
	readbuf.sz = size
	return readbuf
end
function remote_mem_index:write(buf, size, ofs)
	ofs = ofs or 0
	size = math.min(self.sz - ofs, size)
	ffi.copy(self.ptr + ofs, buf, size)
end
function remote_mem_index:seek(whence, ofs)
	exception.raise('invalid', 'operation', 'cannot seek memory rio')
end
function remote_mem_index:close()
	if self.ptr ~= ffi.NULL and self.gc then
		memory.free(self.ptr)
	end
end
ffi.metatype('luact_remote_mem_t', remote_mem_mt)


local default_flag = bit.bor(fs.O_RDONLY)
local default_mode = fs.mode("644")
function _M.file(path, flag, mode)
	return _M.fd(fs.open(path, flag or default_flag, mode or default_mode))
end
function _M.fd(fd)
	local p = memory.alloc_typed('luact_remote_io_t')
	p.fd = fd
	return luact(p)
end
function _M.memory(p, len, gc)
	local p = memory.alloc_typed('luact_remote_mem_t')
	p.ptr = p
	p.len = len
	p.gc = gc
	return luact(p)
end

return _M