local writer = require 'luact.writer'

local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local thread = require 'pulpo.thread'

local _M = {
	read = {},
	write = {},
	writer = writer 
}
local WRITER_NONE = writer.WRITER_NONE

thread.add_initializer(function (loader, shmem)

--[[
	cdef
--]]
loader.load("luact.pbuf.lua", {
	--> from pthread
	"pthread_mutex_t", "struct iovec", 
}, {}, thread.PTHREAD_LIB_NAME, [[
	#include <pthread.h>
	#include <sys/uio.h>
	typedef struct luact_rbuf {
		char *buf;
		size_t max, len, ofs;
	} luact_rbuf_t;
	typedef struct luact_wbuf {
		luact_rbuf_t rbuf[2];
		luact_rbuf_t *curr, *next;
		char *last_p, last_cmd, padd[3];
		pthread_mutex_t mutex[1];
	} luact_wbuf_t;
]])


--[[
 	rbuf 
--]]
local rbuf_index = {}
local rbuf_mt = { __index = rbuf_index }

function rbuf_index:reserve(sz)
	local r = self.max - self.ofs
	local buf
	if r >= sz then return end
	if self.buf ~= ffi.NULL then
		-- create new ptr object and copy unread buffer into it.
		local copyb = self.len - self.ofs;
		local newsz = self.max;
		sz = sz + copyb;
		-- print(copyb, sz, org);
		while sz > newsz do newsz = bit.lshift(newsz, 1) end
		buf = memory.alloc(newsz)
		if buf == ffi.NULL then
			exception.raise('malloc', 'void*', newsz)
		end
		self.max = newsz
		if copyb > 0 then 
			ffi.copy(buf, self.buf + self.ofs, copyb)
		end
		self.ofs = 0;
		self.len = copyb
	else
		self.max = INITIAL_BUFFER_SIZE;
		-- now self.buf == NULL, means no self.ofs, self.len. so compare with self.max work fine. 
		while sz > self.max do self.max = bif.lshift(self.max, 1) end
		buf = memory.alloc(self.max)
		if buf == ffi.NULL then
			exception.raise('malloc', 'void*', self.max)
		end
	end
	self.buf = buf
end
function rbuf_index:read(io, size)
	self:reserve(size)
	self.ofs = self.ofs + io:read(self.buf + self.ofs, size)
end
function rbuf_index:push(byte)
	self.buf[self.ofs] = byte
	self.ofs = self.ofs + 1
end
function rbuf_index:available()
	return self.len - self.ofs
end
function rbuf_index:curr_p()
	return self.buf + self.ofs
end
function rbuf_index:last_p()
	return self.buf + self.max
end
function rbuf_index:shrink(sz)
	if sz >= self.len then
		self.len = 0
	else
		C.memmove(self.buf, self.buf + sz, sz)
		self.len = self.len - sz
	end
end
ffi.metatype('luact_rbuf_t', rbuf_mt)



--[[
 	wbuf
--]]
local wbuf_index = {}
local wbuf_mt = { __index = wbuf_index }

function wbuf_index:init()
	C.pthread_mutex_init(self.mutex, nil)
	self.curr = self.rbuf
	self.next = self.rbuf + 1
	self.last_cmd = WRITER_NONE
end
function wbuf_index:reserve(sz)
	local p = self.next
	local org_p = p.buf
	p:reserve(sz)
	if org_p ~= p.buf then
		self.last_p = (p.buf + (self.last_p - org_p))
	end
end
function wbuf_index:reserve_with_cmd(sz, cmd)
	local append = (cmd == self.last_cmd)
	if append then
		self:reserve(sz)
	else
		self:reserve(sz + 1)
		self.last_p[0] = cmd
		self.last_p = (self.last_p + 1)
	end
	return append
end
function wbuf_index:lock(writer, ...)
	C.pthread_mutex_lock(self.mutex)
	local ok, r = pcall(writer, ...)
	C.pthread_mutex_unlock(self.mutex)
	assert(ok, r)
	return r
end
function wbuf_index:send(writer, ...)
	self:lock(self.do_send, self, writer, ...)
end
function wbuf_index:do_send(w, ...)
	local sz = w.write(self, ...)
	if self.last_cmd == WRITER_NONE then
		self.io:reactivate_write() -- remove and add edge trigger.
		-- so even no actual environment changed, edge trigger should be triggered again.
		-- TODO : any faster way than invoking syscall twice?
	end
	self.last_cmd = w.cmd
	return sz
end
function wbuf_index:write(io)
	if self.curr:available() == 0 then
		if not self:lock(self.swap, self) then
			-- because pulpo's polling always edge triggered, 
			-- it waits for triggering edged write event caused by calling io:reactivate_write()
			-- most of case. (see wbuf_index.do_send)
			-- even if reactivate_write calls before pulpo.event.wait_write, it works.
			-- because next epoll_wait or kqueue call will be done after this coroutine yields 
			-- thus, pulpo.event.wait_write always calls before polling syscall.
			pulpo.event.wait_write(self.io)
			-- wait until next wbuf_index.do_send called...
		end
	else
		local r
		local now, last = self.curr:curr_p(), self.curr:last_p()
		while now < last do
			local w = ffi.cast(writer[now[0]], now + 1)
			local r = w:syscall(io)
			w:sent(r)
			if w:finish() then
				now = now + w:chunk_size()
			else
				goto shrink_buffer
			end
		end
	::shrink_buffer::
		assert(self.next ~= ffi.NULL and self.curr ~= ffi.NULL)
		assert(now >= self.curr:curr_p());	-- if no w->finish(), == is possible 
		self.curr:shrink(now - ctx.curr:curr_p())
	end
end
function wbuf_index:swap()
	local tmp = self.curr
	self.curr = self.next
	self.next = tmp
	if self.curr:available() == 0 then
		-- it means there is no more packet to send at this timing
		-- calling wbuf_index:do_send from any thread, resumes this yield
		self.last_cmd = WRITER_NONE
		return false
	end
	return true
end
ffi.metatype('luact_wbuf_t', wbuf_mt)

end) -- thread.add_initializer

--[[
 	create read buf/write buf
--]]
function _M.read.new()
	local p = memory.managed_alloc_typed('luact_rbuf_t')
	return p
end
function _M.write.new()
	local p = memory.managed_alloc_typed('luact_wbuf_t')
	p:init()
	return p
end

return _M
