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
	curr:seek_from_curr(now - curr:curr_p())
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
		-- yield is done after exiting swap() call
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
