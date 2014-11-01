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

local WRITER_NONE = writer.WRITER_NONE
local WRITER_RAW = writer.WRITER_RAW
local WRITER_VEC = writer.WRITER_VEC

local INITIAL_BUFFER_SIZE = 1024

--[[
	cdef
--]]
ffi.cdef([[
	typedef struct luact_rbuf {
		char *buf;
		luact_bufsize_t max, used, hpos;
	} luact_rbuf_t;
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
		local copyb = self.used - self.hpos
		local newsz = self.max
		sz = sz + copyb
		-- print(copyb, sz, org)
		while sz > newsz do newsz = (newsz * 2) end
		buf = memory.alloc(newsz)
		if buf == util.NULL then
			exception.raise('malloc', 'void*', newsz)
		end
		self.max = newsz
		if copyb > 0 then 
			ffi.copy(buf, self.buf + self.hpos, copyb)
		end
		self.hpos = 0
		self.used = copyb
		memory.free(self.buf)
	else
		self.max = INITIAL_BUFFER_SIZE
		-- now self.buf == NULL, means no self.ofs, self.len. so compare with self.max work fine. 
		while sz > self.max do self.max = (self.max * 2) end
		buf = memory.alloc(self.max)
		if buf == util.NULL then
			exception.raise('malloc', 'void*', self.max)
		end
	end
	self.buf = buf
end
function rbuf_index:reserve_with_cmd(sz, cmd)
	self:seek_from_last(0)
	self:reserve(sz + 1)
	self.buf[self.used] = cmd
	self.used = self.used + 1
end
function rbuf_index:read(io, size)
	self:reserve(size)
	self.used = self.used + io:read(self.buf + self.used, size)
end
function rbuf_index:available()
	return self.used - self.hpos
end
function rbuf_index:use(r)
	self.used = self.used + r
end
function rbuf_index:seek_from_curr(r)
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
function rbuf_index:dump()
	io.write('buffer:'..tostring(self.buf))
	for i=0,tonumber(self.used)-1,1 do
		io.write(":")
		io.write(string.format('%02x', self.buf[i]))
	end
	io.write('\n')
end
function rbuf_index:shrink(sz)
	if sz >= self.used then
		self.used = 0
	else
		self.used = self.used - sz
		C.memmove(self.buf, self.buf + sz, self.used)
	end
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
	self.last_cmd = WRITER_NONE
end
function wbuf_index:reset()
	self.rbuf[0]:reset()
	self.rbuf[1]:reset()
	self.last_cmd = WRITER_NONE
end
function wbuf_index:fin()
	self.rbuf[0]:fin()
	self.rbuf[1]:fin()
end
function wbuf_index:set_io(io)
	self.io = io
end
function wbuf_index:send(w, ...)
	local sz = w.write(self.next, self.last_cmd == w.cmd, ...)
	if self.last_cmd == WRITER_NONE then
		self.io:reactivate_write() -- add edge trigger again.
		-- so even no actual environment changed, edge trigger should be triggered again.
	end
	self.last_cmd = w.cmd
	return sz
end
function wbuf_index:write()
	local curr = self.curr
	while curr:available() == 0 do
		if not self:swap() then
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
			-- wait until next wbuf_index.do_send called...
		end
	end
	local r
	local now, last = curr:curr_p(), curr:last_p()
	while now < last do
		-- print(_M.writer[now[0]], now[0], curr:available())
		local w = ffi.cast(_M.writer[now[0]], now + 1)
		-- print(w, ffi.string(w.p))
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
	curr:shrink(now - curr:curr_p())
end
function wbuf_index:swap()
	local tmp = self.curr
	self.curr = self.next
	self.next = tmp
	if self.curr:available() == 0 then
		-- it means there is no more packet to send at this timing
		-- calling wbuf_index:do_send from any thread, resumes this yield
		self.last_cmd = WRITER_NONE
		self.io:deactivate_write()
		-- yield is done after exiting swap() call, because it usually called inside mutex lock.
		return false
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