-- force preload luact.writer first, which is dependency of pbuf.
local writer = require 'luact.writer'
local ffi = require 'ffiex.init'
local memory = require 'pulpo.memory'

local require_on_boot = (require 'pulpo.package').require
local _M = require_on_boot 'luact.defer.pbuf_c'

-- predefine luact_rbuf_t, because it refers many other modules
ffi.cdef[[
	typedef struct luact_rbuf {
		union {
			char *buf;
			uint8_t *ubuf;
		};
		luact_bufsize_t max, used, hpos;
	} luact_rbuf_t;
]]


--[[
 	rbuf 
--]]
local INITIAL_BUFFER_SIZE = 1024

local rbuf_index = {}
local rbuf_mt = { __index = rbuf_index }

function rbuf_index:init()
	self.buf = ffi.NULL
	self.max, self.used, self.hpos = 0, 0, 0
end
function rbuf_index:reset()
	self.used, self.hpos = 0, 0
	self:reduce_malloc_size()
end
function rbuf_index:fin()
	if self.buf ~= ffi.NULL then
		memory.free(self.buf)
	end
end
function rbuf_index:reserve(sz)
	local r = self.max - self.used
	local buf
	if r >= sz then return end
	if self.buf ~= ffi.NULL then
		-- create new ptr object and copy unread buffer into it.
		local copyb = self.used
		local newsz = self.max
		sz = sz + copyb
		while sz > newsz do newsz = (newsz * 2) end
		buf = memory.realloc(self.buf, newsz)
		if not buf then
			exception.raise('malloc', 'void*', newsz)
		end
		logger.info('reserve1 ptr:', r, self.max, self.used, sz, self.buf, '=>', buf, copyb, self.used, self.hpos, self.max, newsz)
		self.max = newsz
		self.used = copyb
	else
		self.max = INITIAL_BUFFER_SIZE
		-- now self.buf == NULL, means no self.ofs, self.len. so compare with self.max work fine. 
		while sz > self.max do self.max = (self.max * 2) end
		buf = memory.alloc(self.max)
		if not buf then
			exception.raise('malloc', 'void*', self.max)
		end
	end
	self.buf = buf
end
function rbuf_index:reserve_and_reduce_unused(sz)
	local r = self.max - self.used
	local buf
	if r >= sz then return end
	if self.buf ~= ffi.NULL then
		-- create new ptr object and copy unread buffer into it.
		-- also reduce used memory block with self.hpos size (because its already read)
		local copyb = self.used - self.hpos
		local newsz = self.max
		sz = sz + copyb
		while sz > newsz do newsz = (newsz * 2) end
		buf = memory.alloc(newsz)
		if not buf then
			exception.raise('malloc', 'void*', newsz)
		end
		self.max = newsz
		if copyb > 0 then 
			ffi.copy(buf, self.buf + self.hpos, copyb)
		end
		logger.info('reserve2 ptr:', sz, self.buf, '=>', buf, copyb, self.used, self.hpos, self.max)
		self.hpos = 0
		self.used = copyb
		memory.free(self.buf)
	else
		self.max = INITIAL_BUFFER_SIZE
		-- now self.buf == NULL, means no self.ofs, self.len. so compare with self.max work fine. 
		while sz > self.max do self.max = (self.max * 2) end
		buf = memory.alloc(self.max)
		if not buf then
			exception.raise('malloc', 'void*', self.max)
		end
	end
	self.buf = buf	
end
function rbuf_index:reserve_with_cmd(sz, cmd)
	self:reserve(sz + 1)
	self.buf[self.used] = cmd
	self.used = self.used + 1
	self:seek_from_last(0)
end
function rbuf_index:read(io, size)
	self:reserve_and_reduce_unused(size)
	local len = io:read(self.buf + self.used, size)
	if len then
		self.used = self.used + len
		return true
	end
end
function rbuf_index:available()
	return self.used - self.hpos
end
function rbuf_index:use(r)
	self.used = self.used + r
end
function rbuf_index:seek_from_curr(r)
	if self.hpos > self.used then
		logger.report('invalid seek', self.hpos, self.used, debug.traceback())
		assert(false)
	end
	-- logger.info('seek_from_curr', self.hpos, r, self.hpos + r, self.used)
	self.hpos = self.hpos + r
end
function rbuf_index:seek_from_start(r)
	self.hpos = r
end
function rbuf_index:seek_from_last(r)
	self.hpos = self.used - r
end
function rbuf_index:curr_p()
	return self.buf + self.hpos
end
function rbuf_index:curr_byte_p()
	return self.ubuf + self.hpos
end
function rbuf_index:last_p()
	return self.buf + self.used
end
function rbuf_index:last_byte_p()
	return self.ubuf + self.used
end
function rbuf_index:start_p()
	return self.buf
end
function rbuf_index:dump(full)
	io.write('max,used,hpos: '..tonumber(self.max)..' '..tonumber(self.used)..' '..tonumber(self.hpos)..'\n')
	io.write('buffer:'..tostring(self.buf))
	for i=0,tonumber(self.used)-1,1 do
		io.write(":")
		io.write(string.format('%02x', tonumber(ffi.cast('unsigned char', self.buf[i]))))
	end
	io.write('\n')
	if full then
		print('as string', '['..ffi.string(self.buf, self.used)..']')
	end
end
function rbuf_index:shrink(sz)
	-- TODO : in this timing, shrink malloc'ed size also, if it is too large.
	if sz >= self.used then
		self.used = 0
	else
		self.used = self.used - sz
		memory.move(self.buf, self.buf + sz, self.used)
	end
end
function rbuf_index:shrink_by_hpos()
	self:shrink(self.hpos)
	self.hpos = 0
	self:reduce_malloc_size()
end
_M.LARGE_ALLOCATION_THRESHOLD = 256 * 1024
function rbuf_index:reduce_malloc_size()
	if self.max >= _M.LARGE_ALLOCATION_THRESHOLD then
		logger.notice('shrink rbuf', self.max)
		local buf = memory.realloc(self.buf, math.max(tonumber(self.used), INITIAL_BUFFER_SIZE))
		if not buf then return end
		self.buf = buf
		self.max = INITIAL_BUFFER_SIZE
	end
end
ffi.metatype('luact_rbuf_t', rbuf_mt)

return _M
