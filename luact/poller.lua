-- actor main loop
local ffi = require 'ffiex'
local thread = require 'luact.thread'
local memory = require 'luact.memory'
local util = require 'luact.util'

local _M = {}
local poller_cdecl, poller_index, event_index = nil, {}, {}
local handlers = {}


---------------------------------------------------
---------------------------------------------------
-- common types for method of polling
---------------------------------------------------
---------------------------------------------------
local read_handlers, write_handlers, gc_handlers= {}, {}, {}
local handler_type_cdata = {}

--> ctype metatable
function _M.add_handler(type, reader, writer, gc)
	assert(not handler_type_cdata[type], "handler already registered:"..tostring(type))
	handler_type_cdata[type] = ffi.new('int', type)
	read_handlers[type] = reader
	write_handlers[type] = writer
	gc_handlers[type] = gc
end

---------------------------------------------------
---------------------------------------------------
-- system dependent initialization
---------------------------------------------------
---------------------------------------------------
function init_library(opts)
	---------------------------------------------------
	-- system which depends on kqueue for polling
	---------------------------------------------------
	if ffi.os == "OSX" then
	local ffi_state,clib = thread.import("poller.lua", {
		"kqueue", "kevent", "socklen_t", "sockaddr_in", 
	}, {
		"EV_ADD", "EV_ENABLE", "EV_DISABLE", "EV_DELETE", "EV_RECEIPT", "EV_ONESHOT",
		"EV_CLEAR", "EV_EOF", "EV_ERROR",
		"EVFILT_READ", 
		"EVFILT_WRITE", 
		"EVFILT_AIO", 
		"EVFILT_VNODE", 
			"NOTE_DELETE", -->		The unlink() system call was called on the file referenced by the descriptor.
			"NOTE_WRITE", -->		A write occurred on the file referenced by the descriptor.
			"NOTE_EXTEND", -->		The file referenced by the descriptor was extended.
			"NOTE_ATTRIB", -->		The file referenced by the descriptor had its attributes changed.
			"NOTE_LINK", -->		The link count on the file changed.
			"NOTE_RENAME", -->		The file referenced by the descriptor was renamed.
			"NOTE_REVOKE", -->		Access to the file was revoked via revoke(2) or the underlying fileystem was unmounted.
		"EVFILT_PROC",
			"NOTE_EXIT", -->		The process has exited.
	        "NOTE_EXITSTATUS",--[[	The process has exited and its exit status is in filter specific data.
								  	Valid only on child processes and to be used along with NOTE_EXIT. ]]
			"NOTE_FORK", -->    	The process created a child process via fork(2) or similar call.
			"NOTE_EXEC", -->    	The process executed a new process via execve(2) or similar call.
			"NOTE_SIGNAL", -->  	The process was sent a signal. Status can be checked via waitpid(2) or similar call.
		"EVFILT_SIGNAL", 
		"EVFILT_MACHPORT", 
		"EVFILT_TIMER", 
			"NOTE_SECONDS", -->   	data is in seconds
			"NOTE_USECONDS", -->  	data is in microseconds
			"NOTE_NSECONDS", -->  	data is in nanoseconds
			"NOTE_ABSOLUTE", -->  	data is an absolute timeout
			"NOTE_CRITICAL", -->  	system makes a best effort to fire this timer as scheduled.
			"NOTE_BACKGROUND", -->	system has extra leeway to coalesce this timer.
			"NOTE_LEEWAY", -->    	ext[1] holds user-supplied slop in deadline for timer coalescing.
	}, nil, [[
		#include <sys/event.h>
		#include <sys/time.h>
		#include <sys/socket.h>
		#include <netinet/in.h>
	]])

	poller_cdecl = function (maxfd) 
		return ([[
			typedef int luact_fd_t;
			typedef struct kevent luact_event_t;
			typedef luact_event_t luact_io_t;
			typedef struct poller {
				bool alive;
				luact_fd_t kqfd;
				luact_io_t iolist[%d]:
				luact_event_t changes[%d];
				int nchanges;
				luact_event_t events[%d];
				int nevents;
				struct timespec timeout[1];
				int maxfd;
			} luact_poller_t;
		]]):format(maxfd, maxfd, maxfd)
	end

	local EVFILT_READ = ffi_state.defs.EVFILT_READ
	local EVFILE_WRITE = ffi_state.defs.EVFILT_WRITE

	local EV_ADD = ffi_state.defs.EV_ADD
	local EV_ONESHOT = ffi_state.defs.EV_ONESHOT
	local EV_DELETE = ffi_state.defs.EV_DELETE

	--> ctype metatable (system dependent version)
	--[[
		struct kevent {
			uintptr_t ident;        /* このイベントの識別子 */
			short     filter;       /* イベントのフィルタ */
			u_short   flags;        /* kqueue のアクションフラグ */
			u_int     fflags;       /* フィルタフラグ値 */
			intptr_t  data;         /* フィルタデータ値 */
			void      *udata;       /* 不透明なユーザデータ識別子 */
		};
	]]
	--> luact_event_t
	function event_index.init(t, fd, type, ctx)
		t.flags = bit.band(EV_ADD, EV_ONESHOT)
		t.ident = fd
		local ctxp = assert(memory.alloc_typed('luact_context_t'), "malloc fails")
		ctxp.type = type
		ctxp.ctx = ctx or ffi.NULL
		t.udata = ffi.cast('void *', ctxp)
	end
	function event_index.read(t, ptr, len)
		return read_handlers[t:type()](t, ptr, len)
	end
	function event_index.wait_read(t)
		t.filter = EVFILT_READ
		ffi.copy(t, coroutine.yield(t))
	end
	function event_index.write(t, ptr, len)
		return write_handlers[t:type()](t, ptr, len)
	end
	function event_index.wait_write(t)
		t.filter = EVFILT_WRITE
		ffi.copy(t, coroutine.yield(t))
	end
	function event_index.add_to(t, poller)
		assert(bit.band(t.flags, EV_ADD), "invalid event flag")
		local n = C.kqueue(poller.kqfd, t, 1, nil, 0, nil)
		if n ~= 0 then
			print('kqueue event change error:'..ffi.errno())
			return false
		end
		return true
	end
	function event_index.remove_from(t, poller)
		assert(bit.band(t.flags, EV_DELETE), "invalid event flag")
		local n = C.kqueue(poller.kqfd, t, 1, nil, 0, nil)
		if n ~= 0 then
			print('kqueue event change error:'..ffi.errno())
			return false
		end
		gc_handlers[t:type()](t)
		if t.udata ~= ffi.NULL then
			memory.free(t.udata)
		end
	end
	function event_index.fd(t)
		return t.ident
	end
	function event_index.type(t)
		return tonumber(ffi.cast('luact_context_t*', t.udata).type)
	end
	function event_index.ctx(t, type)
		return ffi.cast(type, ffi.cast('luact_context_t*', t.udata).ctx)
	end
	function event_index.by(t, poller, cb)
		return poller:add(t, cb)
	end


	--> luact_poller_t
	function poller_index.init(t)
		t.kqfd = assert(C.kqueue() < 0, "kqueue create fails")
		t.alive = true
		t:set_timeout(0.05) --> default 50ms
	end
	function poller_index.fin(t)
		C.close(t.poller_fd)
	end
	function poller_index.add(t, ev, co)
		if not ev:add_to(t) then return false end
		handlers[tonumber(ev.ident)] = (
			(type(co) == "function") and coroutine.wrap(co) or co
		)
		local p = t.iolist + ev:fd()	
		ffi.copy(p, ev, ffi.sizeof(ev))
		co(p)
		return true
	end
	function poller_index.remove(t, ev)
		if not ev:remove_from(t) then return false end
		local fd = ev:nfd()
		handlers[fd] = nil
		ffi.fill(t.iolist + fd, ffi.sizeof(ev)) --> bzero
		return true
	end
	function poller_index.set_timeout(t, sec)
		util.sec2timespec(sec, t.timeout)
	end
	function poller_index.wait(t)
		local n = C.kqueue(t.kqfd, nil, 0, t.events, t.nevents, t.timeout)
		if n < 0 then
			print('kqueue error:'..ffi.errno())
			return
		end
		for i=0,n-1,1 do
			local ev = t.events + i
			local fd = tonumber(ev.ident)
			local co = assert(handlers[fd], "handler should exist for fd:"..tostring(fd))
			local rev = co(ev)
			if rev then
				rev:add_to(t)
			else
				t:remove(ev)
			end
		end
	end
	function poller_index.start(t)
		while t:alive() do
			t:wait()
		end
	end
	function poller_index.alive(t)
		return t.alive
	end
	function poller_index.stop(t)
		t.alive = false
	end


	---------------------------------------------------
	---------------------------------------------------
	-- system which depends on epoll for polling
	---------------------------------------------------
	---------------------------------------------------
	elseif ffi.os == "Linux" then
	thread.import("poller.lua", {
		"epoll_create", "epoll_wait", "epoll_ctl",
	}, {
		"EPOLL_CTL_ADD", "EPOLL_CTL_MOD", "EPOLL_CTL_DEL",
		"EPOLLIN", "EPOLLOUT", "EPOLLRDHUP", "EPOLLPRI", "EPOLLERR", "EPOLLHUP",
		"EPOLLET", "EPOLLONESHOT"  
	}, nil, [[
		#include <sys/epoll.h>
	]])



	else
		error("unsupported platform:"..ffi.os)
	end
end

---------------------------------------------------
---------------------------------------------------
-- module body
---------------------------------------------------
---------------------------------------------------
--> module poller 

function _M.initialize(opts)
	-- system dependent initialization
	init_library(opts)
	
	-- system dependent initialization
	_M.maxfd = util.maxfd(opts.maxfd or 1024)
	ffi.cdef(poller_cdecl(_M.maxfd))
	ffi.metatype('luact_poller_t', { __index = poller_index })
	ffi.metatype('luact_event_t', { __index = event_index })
	ffi.metatype('luact_io_t', { __index = event_index })
	return true
end

function _M.new()
	local p = memory.alloc_typed('luact_poller_t')
	p.maxfd = _M.maxfd
	p:init()
	return p
end

function _M.newio(fd, type, ctx)
	local io = memory.alloc_typed('luact_io_t')
	io:init(fd, type, ctx)
	return io
end


return _M
