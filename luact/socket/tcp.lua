local ffi = require 'ffiex'
local poller = require 'luact.poller'
local util = require 'luact.util'
local memory = require 'luact.memory'
local C = ffi.C

local _M = {}

--> cdef
local EAGAIN = ffi.defs.EAGAIN
local EWOULDBLOCK = ffi.defs.EWOULDBLOCK
local ENOTCONN = ffi.defs.ENOTCONN
ffi.cdef [[
typedef struct luact_tcp_context {
	struct sockaddr_in addr;
	socklen_t alen[1];
} luact_tcp_context_t;
]]


function tcp_connect(io)
	local ctx = io:ctx('luact_tcp_context_t')
	local n = C.connect(io:fd(), ctx.addr, ctx.alen[0])
	if n < 0 then
		error(('tcp connect fails(%d) on %d'):format(ffi.errno(), io:fd()))
	end
end

function tcp_server_socket(fd, ctx)
	return poller.newio(fd, HANDLER_TYPE_TCP, ctx)	
end


--> handlers
function tcp_read(io, ptr, len)
::retry::
	local n = C.read(io:fd(), ptr, len)
	if n < 0 then
		local eno = ffi.errno()
		if eno == EAGAIN or eno == EWOULDBLOCK then
			io:wait_read()
			goto retry
		elseif eno == ENOTCONN then
			tcp_connect(io)
			goto retry
		else
			error(('tcp read fails(%d) on %d'):format(eno, io:fd()))
		end
	end
	return n
end

function tcp_write(io, ptr, len)
::retry::
	local n = C.write(io:fd(), ptr, len)
	if n < 0 then
		local eno = ffi.errno()
		if eno == EAGAIN or eno == EWOULDBLOCK then
			io:wait_write()
			goto retry
		elseif eno == ENOTCONN then
			tcp_connect(io)
			goto retry
		else
			error(('tcp write fails(%d) on %d'):format(eno, io:fd()))
		end
	end
	return n
end

function tcp_accept(io)
::retry::
	local n = C.accept(io:fd(), ctx.addr, ctx.alen)
	if n < 0 then
		local eno = ffi.errno()
		if eno == EAGAIN or eno == EWOULDBLOCK then
			io:wait_read()
			goto retry
		else
			error(('tcp write fails(%d) on %d'):format(eno, io:fd()))
		end
	end
	return tcp_server_socket(n, ctx)
end

function tcp_gc(io)
	C.close(io:fd())
end


local HANDLER_TYPE_TCP = 1
local HANDLER_TYPE_TCP_LISTENER = 2
poller.add_hundler(HANDLER_TYPE_TCP, tcp_read, tcp_write, tcp_gc)
poller.add_hundler(HANDLER_TYPE_TCP_LISTENER, tcp_accept, nil, tcp_gc)

--> TODO : configurable
function _M.configure(opts)
	_M.opts = opts
end

function _M.connect(addr)
	local fd = C.socket(AF_INET, SOCK_STREAM, 0)
	local ctx = memory.alloc_typed('luact_tcp_context_t')
	ctx.alen[0] = util.inet_hostbyname(addr, ctx.addr, ffi.sizeof(ctx.addr))
	return poller.newio(fd, HANDLER_TYPE_TCP, ctx)
end


local TCP_LISTEN_BACKLOG = (512)
--[[ 
	if os somaxconn is less than TCP_LISTEN_BACKLOG, increase value by
	linux:	sudo /sbin/sysctl -w net.core.somaxconn=NBR_TCP_LISTEN_BACKLOG
			(and sudo /sbin/sysctl -w net.core.netdev_max_backlog=3000)
	osx:	sudo sysctl -w kern.ipc.somaxconn=NBR_TCP_LISTEN_BACKLOG
	(from http://docs.codehaus.org/display/JETTY/HighLoadServers)
]]
if ffi.os == "Linux" then
io.popen(('sudo /sbin/sysctl -w net.core.somaxconn=%d'):format(TCP_LISTEN_BACKLOG))
elseif ffi.os == "OSX" then
io.popen(('sudo /sbin/sysctl -w kern.ipc.somaxconn=%d'):format(TCP_LISTEN_BACKLOG))
end
function _M.listen(addr)
	local fd = C.socket(AF_INET, SOCK_STREAM, 0)
	local reuse = ffi.new('int[1]', 1)
	if C.setsockopt(fd, ffi.defs.SOL_SOCKET, ffi.defs.SO_REUSEADDR, reuse, ffi.sizeof(reuse)) < 0 then
		error('fail to listen:setsockopt:'..ffi.errno())
	end
	local sa = memory.managed_alloc_typed('struct sockaddr_in[1]')
	local alen = util.inet_hostbyname(addr, sa, ffi.sizeof(sa))
	if C.bind(fd, sa, alen) < 0 then
		error('fail to listen:bind:'..ffi.errno())
	end
	C.listen(fd, TCP_LISTEN_BACKLOG)
	return poller.newio(HANDLER_TYPE_TCP_LISTENER, fd)
end

return _M
