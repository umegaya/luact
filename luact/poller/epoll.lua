local thread = require 'luact.thread'
local ffi = require 'ffiex'
local util = require 'luact.util'
local memory = require 'luact.memory'

local _M = {}
local C = ffi.C
local iolist = ffi.NULL
local handlers
local read_handlers, write_handlers, gc_handlers, error_handlers

---------------------------------------------------
-- import necessary cdefs
---------------------------------------------------
thread.import("epoll.lua", {
	"epoll_create", "epoll_wait", "epoll_ctl",
}, {
	"EPOLL_CTL_ADD", "EPOLL_CTL_MOD", "EPOLL_CTL_DEL",
	"EPOLLIN", "EPOLLOUT", "EPOLLRDHUP", "EPOLLPRI", "EPOLLERR", "EPOLLHUP",
	"EPOLLET", "EPOLLONESHOT"  
}, nil, [[
	#include <sys/epoll.h>
]])

local EPOLL_CTL_ADD = ffi_state.defs.EPOLL_CTL_ADD
local EPOLL_CTL_MOD = ffi_state.defs.EPOLL_CTL_MOD
local EPOLL_CTL_DEL = ffi_state.defs.EPOLL_CTL_DEL

local EPOLLIN = ffi_state.defs.EPOLLIN
local EPOLLOUT = ffi_state.defs.EPOLLOUT
local EPOLLRDHUP = ffi_state.defs.EPOLLRDHUP
local EPOLLPRI = ffi_state.defs.EPOLLPRI
local EPOLLERR = ffi_state.defs.EPOLLERR
local EPOLLHUP = ffi_state.defs.EPOLLHUP
local EPOLLONESHOT = ffi_state.defs.EPOLLONESHOT



---------------------------------------------------
-- ctype metatable definition
---------------------------------------------------
local poller_cdecl, poller_index, io_index = nil, {}, {}

---------------------------------------------------
-- luact_io metatable definition
---------------------------------------------------
--[[
typedef union epoll_data {
    void    *ptr;
    int      fd;
    uint32_t u32;
    uint64_t u64;
} epoll_data_t;
struct epoll_event {
    uint32_t     events;    /* epoll イベント */
    epoll_data_t data;      /* ユーザデータ変数 */
};
]]
local udatalist
function io_index.init(t, fd, type, ctx)
	assert(bit.band(t.ev.events, EPOLLERR) or t.ev.data.fd == 0, 
		"already used event buffer:"..tonumber(t.ev.ident))
	t.ev.events = bit.bor(EPOLLIN, EPOLLONESHOT)
	t.ev.data.fd = fd
	udatalist[fd] = ffi.cast('void *', ctx)
	t.kind = type
end
function io_index.fin(t)
	t.ev.flags = EPOLLERR
	gc_handlers[t:type()](t)
end
function io_index.read(t, ptr, len)
	return read_handlers[t:type()](t, ptr, len)
end
function io_index.wait_read(t)
	t.ev.events = bit.bor(EPOLLOUT, EPOLLONESHOT)
	-- if log then print('wait_read', t:fd()) end
	local r = coroutine.yield(t)
	-- if log then print('wait_read returns', t:fd()) end
	t.ev.events = r.events
end
function io_index.write(t, ptr, len)
	return write_handlers[t:type()](t, ptr, len)
end
function io_index.wait_write(t)
	t.ev.events = bit.bor(EPOLLIN, EPOLLONESHOT)
	-- if log then print('wait_write', t:fd()) end
	local r = coroutine.yield(t)
	-- if log then print('wait_write returns', t:fd()) end
	t.ev.events = r.events
end
function io_index.add_to(t, poller)
	local n = C.epoll_ctl(poller.kqfd, EPOLL_CTL_ADD, t:fd(), t.ev)
	if n ~= 0 then
		print('epoll event add error:'..ffi.errno().."\n"..debug.traceback())
		return false
	end
	return true
end
function io_index.remove_from(t, poller)
	local n = C.epoll_ctl(poller.kqfd, EPOLL_CTL_DEL, t:fd(), t.ev)
	if n ~= 0 then
		print('epoll event remove error:'..ffi.errno().."\n"..debug.traceback())
		return false
	end
	gc_handlers[t:type()](t)
end
function io_index.fd(t)
	return t.ev.data.fd
end
function io_index.nfd(t)
	return tonumber(t.ev.data.fd)
end
function io_index.type(t)
	return tonumber(t.kind)
end
function io_index.ctx(t, ct)
	local pd = udatalist + t:fd()
	return pd ~= ffi.NULL and ffi.cast(ct, pd) or nil
end
function io_index.by(t, poller, cb)
	return poller:add(t, cb)
end


---------------------------------------------------
-- luact_poller metatable definition
---------------------------------------------------

local function run(t, co, ev, io)
	local ok, rev = pcall(co, ev)
	if ok then
		if rev then
			if rev:add_to(t) then
				return
			end
		end
	else
		print('abort by error:', rev)
	end
	io:fin()
end

function poller_index.init(t, maxfd)
	t.epfd = C.epoll_create(maxfd)
	assert(t.epfd >= 0, "kqueue create fails:"..ffi.errno())
	-- print('kqfd:', tonumber(t.kqfd))
	t.maxfd = maxfd
	t.nevents = maxfd
	t.alive = true
	t:set_timeout(0.05) --> default 50ms
end
function poller_index.fin(t)
	C.close(t.epfd)
end
function poller_index.add(t, io, co)
	co = ((type(co) == "function") and coroutine.wrap(co) or co)
	handlers[tonumber(io:fd())] = co
	run(t, co, io, io)
	return true
end
function poller_index.remove(t, io)
	if not io:remove_from(t) then return false end
	handlers[tonumber(io:fd())] = nil
	return true
end
function poller_index.set_timeout(t, sec)
	t.timeout = sec * 1000 -- msec
end
function poller_index.wait(t)
	local n = C.epoll_wait(t.epfd, t.events, t.nevents, t.timeout)
	if n < 0 then
		print('epoll error:'..ffi.errno())
		return
	end
	--if n > 0 then
	--	print('n = ', n)
	--end
	for i=0,n-1,1 do
		local ev = t.events + i
		local fd = tonumber(ev.data.fd)
		local co = assert(handlers[fd], "handler should exist for fd:"..tostring(fd))
		run(t, co, ev, iolist + fd)
	end
end
function poller_index.newio(t, fd, type, ctx)
	return newio(t, fd, type, ctx)
end
function poller_index.start(t)
	while t.alive do
		t:wait()
	end
end
function poller_index.stop(t)
	t.alive = false
end

---------------------------------------------------
-- main poller ctype definition
---------------------------------------------------
poller_cdecl = function (maxfd) 
	return ([[
		typedef int luact_fd_t;
		typedef struct epoll_event luact_event_t;
		typedef struct luact_io {
			luact_event_t ev;
			unsigned char kind, padd[3];
		} luact_io_t;
		typedef struct poller {
			bool alive;
			luact_fd_t kqfd;
			luact_event_t events[%d];
			int nevents;
			int timeout;
			int maxfd;
		} luact_poller_t;
	]]):format(maxfd)
end

function _M.initialize(args)
	--> copy handlers in poller (as upvalues for above)
	handlers = args.handlers
	read_handlers = args.read_handlers
	write_handlers = args.write_handlers
	gc_handlers = args.gc_handlers
	error_handlers = args.error_handlers

	--> generate run time cdef
	ffi.cdef(poller_cdecl(args.poller.maxfd))
	ffi.metatype('luact_poller_t', { __index = poller_index })
	ffi.metatype('luact_io_t', { __index = io_index })

	--> TODO : share it between threads (but thinking of cache coherence, may better seperated)
	iolist = args.opts.iolist or memory.alloc_fill_typed('luact_io_t', args.opts.maxfd)
	udatalist = memory.alloc_fill_typed('void *', args.opts.maxfd)
	return iolist
end

return _M
