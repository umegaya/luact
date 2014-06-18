local ffi = require 'ffiex'
local poller = require 'luact.poller'
local thread = require 'luact.thread'
local util = require 'luact.util'
local memory = require 'luact.memory'
local errno = require 'luact.errno'
local socket = require 'luact.socket'

local C = ffi.C
local _M = {}

local HANDLER_TYPE_TCP, HANDLER_TYPE_TCP_LISTENER

--> cdef
local EAGAIN = errno.EAGAIN
local EPIPE = errno.EPIPE
local EWOULDBLOCK = errno.EWOULDBLOCK
local ENOTCONN = errno.ENOTCONN
local ECONNREFUSED = errno.ECONNREFUSED
local ECONNRESET = errno.ECONNRESET
local EINPROGRESS = errno.EINPROGRESS
local EINVAL = errno.EINVAL

ffi.cdef [[
typedef struct luact_tcp_context {
	luact_addrinfo_t addrinfo;
} luact_tcp_context_t;
]]


--> helper function
function tcp_connect(io)
::retry::
	local ctx = io:ctx('luact_tcp_context_t*')
	local n = C.connect(io:fd(), ctx.addrinfo.addrp, ctx.addrinfo.alen[0])
	if n < 0 then
		local eno = errno.errno()
		if eno == EINPROGRESS then
			-- print('EINPROGRESS:to:', socket.inet_namebyhost(ctx.addrinfo.addrp))
			io:wait_write()
			return
		elseif eno == ECONNREFUSED then
			print('TODO: server listen backlog may exceed: try reconnection', eno)
			thread.sleep(0.1) -- TODO : use lightweight sleep by timer facility
			goto retry
		else
			error(('tcp connect fails(%d) on %d'):format(eno, io:nfd()))
		end
	end
end

function tcp_server_socket(fd, ctx)
	return poller.newio(fd, HANDLER_TYPE_TCP, ctx)	
end


--> handlers
function tcp_read(io, ptr, len)
::retry::
	local n = C.recv(io:fd(), ptr, len, 0)
	if n < 0 then
		local eno = errno.errno()
		if eno == EAGAIN or eno == EWOULDBLOCK then
			io:wait_read()
			goto retry
		elseif eno == ENOTCONN then
			tcp_connect(io)
			goto retry
		else
			error(('tcp read fails(%d) on %d'):format(eno, io:nfd()))
		end
	end
	return n
end

function tcp_write(io, ptr, len)
::retry::
	local n = C.send(io:fd(), ptr, len, 0)
	if n < 0 then
		local eno = errno.errno()
		-- print(io:fd(), 'write fails', n, eno)
		if eno == EAGAIN or eno == EWOULDBLOCK then
			io:wait_write()
			goto retry
		elseif eno == ENOTCONN then
			tcp_connect(io)
			goto retry
		elseif eno == EPIPE then
			--[[ EPIPE: 
				http://www.freebsd.org/cgi/man.cgi?query=listen&sektion=2
				If a connection
			    request arrives with the queue full the client may	receive	an error with
			    an	indication of ECONNREFUSED, or,	in the case of TCP, the	connection
			    will be *silently* dropped.
				I guess if try to write to such an connection, EPIPE may occur.
				because if I increasing listen backlog size, EPIPE not happen.
			]]
			thread.sleep(0.1) -- TODO : use lightweight sleep by timer facility
			tcp_connect(io)
			goto retry
		else
			error(('tcp write fails(%d) on %d'):format(eno, io:nfd()))
		end
	end
	return n
end

local ctx
function tcp_accept(io)
::retry::
	-- print('tcp_accept:', io:fd())
	if not ctx then
		ctx = memory.alloc_typed('luact_tcp_context_t')
	end
	local n = C.accept(io:fd(), ctx.addrinfo.addrp, ctx.addrinfo.alen)
	if n < 0 then
		local eno = errno.errno()
		if eno == EAGAIN or eno == EWOULDBLOCK then
			io:wait_read()
			goto retry
		else
			error(('tcp accept fails(%d) on %d'):format(eno, io:nfd()))
		end
	else
		-- apply same setting as server 
		if socket.setsockopt(n, io:ctx('luact_sockopt_t*')) < 0 then
			C.close(n)
			goto retry
		end
	end
	local tmp = ctx
	ctx = nil
	return tcp_server_socket(n, tmp)
end

function tcp_gc(io)
	memory.free(io:ctx('void*'))
	C.close(io:fd())
end

HANDLER_TYPE_TCP = poller.add_handler(tcp_read, tcp_write, tcp_gc)
HANDLER_TYPE_TCP_LISTENER = poller.add_handler(tcp_accept, nil, tcp_gc)

--> TODO : configurable
function _M.configure(opts)
	_M.opts = opts
end

function _M.connect(addr, opts)
	local ctx = memory.alloc_typed('luact_tcp_context_t')
	local fd = socket.create_stream(addr, opts, ctx.addrinfo)
	if not fd then error('fail to create socket:'..errno.errno()) end
	return poller.newio(fd, HANDLER_TYPE_TCP, ctx)
end

function _M.listen(addr, opts)
	local ai = memory.managed_alloc_typed('luact_addrinfo_t')
	local fd = socket.create_stream(addr, opts, ai)
	if not fd then error('fail to create socket:'..errno.errno()) end
	if socket.set_reuse_addr(fd, true) then
		C.close(fd)
		error('fail to listen:set_reuse_addr:'..errno.errno())
	end
	if C.bind(fd, ai.addrp, ai.alen[0]) < 0 then
		C.close(fd)
		error('fail to listen:bind:'..errno.errno())
	end
	if C.listen(fd, poller.maxconn) < 0 then
		C.close(fd)
		error('fail to listen:listen:'..errno.errno())
	end
	print('listen:', fd, addr)
	return poller.newio(fd, HANDLER_TYPE_TCP_LISTENER, opts)
end

return _M
