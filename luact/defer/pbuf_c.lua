local ffi = require 'ffiex.init'

local writer = require 'luact.writer'

local pulpo = require 'pulpo.init'
local util = require 'pulpo.util'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local thread = require 'pulpo.thread'
local socket = require 'pulpo.socket'
local event = require 'pulpo.event'

local C = ffi.C
local _M = (require 'pulpo.package').module('luact.defer.pbuf_c')
_M.read = {}
_M.write = {}
_M.writer = writer 

local WRITER_DEACTIVATE = writer.WRITER_DEACTIVATE
local WRITER_NONE = writer.WRITER_NONE
local WRITER_RAW = writer.WRITER_RAW
local WRITER_VEC = writer.WRITER_VEC

local INITIAL_BUFFER_SIZE = 1024

--[[
	cdef
--]]
ffi.cdef([[
	typedef struct luact_wbuf {
		luact_rbuf_t *curr, *next;
		luact_rbuf_t rbuf[2];
		char last_cmd, padd[3];
		pulpo_io_t *io;	
	} luact_wbuf_t;
]])


--[[
 	rbuf 
--]]
local rbuf_index = {}
local rbuf_mt = { __index = rbuf_index }

function rbuf_index:init()
	self.buf = ffi.NULL
	self.max, self.used, self.hpos = 0, 0, 0
end
function rbuf_index:reset()
	self.used, self.hpos = 0, 0
end
function rbuf_index:fin()
	if self.buf ~= util.NULL then
		memory.free(self.buf)
	end
end
function rbuf_index:reserve(sz)
	local r = self.max - self.used
	local buf
	if r >= sz then return end
	if self.buf ~= util.NULL then
		-- create new ptr object and copy unread buffer into it.
		local copyb = self.used
		local newsz = self.max
		sz = sz + copyb
		while sz > newsz do newsz = (newsz * 2) end
		buf = memory.realloc(self.buf, newsz)
		if not buf then
			exception.raise('malloc', 'void*', newsz)
		end
		logger.info('reserve1 ptr:', r, self.max, self.used, sz, self.buf, '=>', buf, copyb, self.used, self.hpos, self.max)--, debug.traceback())
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
function rbuf_index:reserve_and_reduce_unsed(sz)
	local r = self.max - self.used
	local buf
	if r >= sz then return end
	if self.buf ~= util.NULL then
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
	self:reserve_and_reduce_unsed(size)
	self.used = self.used + io:read(self.buf + self.used, size)
end
function rbuf_index:available()
	return self.used - self.hpos
end
function rbuf_index:use(r)
	self.used = self.used + r
end
function rbuf_index:seek_from_curr(r, check)
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
function rbuf_index:last_p()
	return self.buf + self.used
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
	if sz >= self.used then
		self.used = 0
	else
		self.used = self.used - sz
		C.memmove(self.buf, self.buf + sz, self.used)
	end
end
function rbuf_index:shrink_by_hpos()
	self:shrink(self.hpos)
	self.hpos = 0
end
ffi.metatype('luact_rbuf_t', rbuf_mt)



--[[
 	wbuf
--]]
local wbuf_index = {}
local wbuf_mt = { __index = wbuf_index }

function wbuf_index:init()
	self.rbuf[0]:init()
	self.rbuf[1]:init()
	self.curr = self.rbuf
	self.next = self.rbuf + 1
	self.last_cmd = WRITER_DEACTIVATE
end
function wbuf_index:reset()
	self.rbuf[0]:reset()
	self.rbuf[1]:reset()
	self.last_cmd = WRITER_DEACTIVATE
end
function wbuf_index:fin()
	self.rbuf[0]:fin()
	self.rbuf[1]:fin()
end
function wbuf_index:set_io(io)
	self.io = io
end
function wbuf_index:send(w, ...)
	w.write(self.next, self.last_cmd == w.cmd, ...)
	if self.last_cmd == WRITER_DEACTIVATE then
		self.io:reactivate_write() -- add edge trigger again.
		-- so even no actual environment changed, edge trigger should be triggered again.
	end
	self.last_cmd = w.cmd
end
function wbuf_index:write()
	-- print('avail:', self.curr:available(), self.next:available())
	if self.curr:available() == 0 then
		-- print('swap current:', self.curr, self.next)
		repeat
			-- there should be no unread buffer (because available() == 0)
			-- TODO : reduce curr buffer size if its too large.
			self.curr:reset()
			-- because pulpo's polling always edge triggered, 
			-- it waits for triggering edged write event caused by calling io:reactivate_write()
			-- most of case. (see wbuf_index.do_send)
			-- even if reactivate_write calls before pulpo.event.wait_write, it works.
			-- because next epoll_wait or kqueue call will be done after this coroutine yields 
			-- thus, pulpo.event.wait_write always calls before polling syscall.
			if not event.wait_reactivate_write(self.io) then
				logger.warn('write thread terminate:', self.io:fd())
				return
			end
			-- wait until next wbuf_index.send called...
		until self:swap()
	end
	local r
	local curr = self.curr
	local now, last = curr:curr_p(), curr:last_p()
	while now < last do
		if not _M.writer[now[0]] then
			curr:dump(true)
			logger.report('enddump', now, now[0], _M.writer[now[0]])--, debug.traceback())
		end
		local w = ffi.cast(_M.writer[now[0]], now + 1)
		local r = w:syscall(self.io)
		w:sent(r)
		if w:finish() then
			-- print(now, w:chunk_size(), last, now + w:chunk_size())
			now = now + w:chunk_size()
		else
			break -- cannot send all buffer
		end
	end
	assert(now >= curr:curr_p());	-- if no w->finish(), == is possible 
	curr:seek_from_curr(now - curr:curr_p(), true)
end
function wbuf_index:swap()
	local tmp = self.curr
	self.curr = self.next
	self.next = tmp
	self.curr:seek_from_start(0) -- rewind hpos to read from start
	if self.curr:available() == 0 then
		-- it means there is no more packet to send at this timing
		-- calling wbuf_index:do_send from any thread, resumes this yield
		self.last_cmd = WRITER_DEACTIVATE
		self.io:deactivate_write()
		-- yield is done after exiting swap() call, because it usually called inside mutex lock.
		return false
	else
		self.last_cmd = WRITER_NONE -- make next write append == false 
	end
	return true
end
ffi.metatype('luact_wbuf_t', wbuf_mt)

--[[
 	create read buf/write buf
--]]
function _M.read.new()
	local p = memory.alloc_typed('luact_rbuf_t')
	p:init()
	return p
end
function _M.write.new(io)
	local p = memory.alloc_typed('luact_wbuf_t')
	p:init(io)
	return p
end

return _M
