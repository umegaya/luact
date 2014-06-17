local loader = require 'luact.loader'
local memory = require 'luact.memory'
local util = require 'luact.util'

local C = ffi.C
local _M = {}

local ffi_state = loader.load("socket.lua", {
	"socket", "connect", "listen", "setsockopt", "bind", "accept", 
	"recv", "send", "recvfrom", "sendto", "close", "getaddrinfo", "freeaddrinfo", "inet_ntop", 
	"fcntl", 
	"luact_bytes_op", "luact_sockopt_t", "luact_addrinfo_t", 
}, {
	"AF_INET", "AF_INET6", 
	"SOCK_STREAM", 
	"SOCK_DGRAM", 
	"SOL_SOCKET", 
		"SO_REUSEADDR", 
		"SO_SNDTIMEO",
		"SO_RCVTIMEO",
		"SO_SNDBUF",
		"SO_RCVBUF",
	"F_GETFL",
	"F_SETFL", 
		"O_NONBLOCK", 
	nice_to_have = {
		"SO_REUSEPORT",
	}, 
}, nil, [[
	#include <sys/socket.h>
	#include <arpa/inet.h>
	#include <netdb.h>
	#include <unistd.h>
	#include <fcntl.h>
	union luact_bytes_op {
		unsigned char p[0];
		unsigned short s;
		unsigned int l;
		unsigned long long ll;
	};
	typedef struct luact_sockopt {
		union {
			char p[sizeof(int)];
			int data;
		} rblen;
		union {
			char p[sizeof(int)];
			int data;
		} wblen;
		int timeout;
		bool blocking;
	} luact_sockopt_t;
	typedef struct luact_addrinfo {
		union {
			struct sockaddr_in addr4;
			struct sockaddr_in6 addr6;
			struct sockaddr addrp[1];
		};
		socklen_t alen[1];
	} luact_addrinfo_t;

]])

-- TODO : current 'inet_namebyhost' implementation assumes binary layout of sockaddr_in and sockaddr_in6, 
-- is the same at first 4 byte (sa_family and sin_port) 
assert(ffi.offsetof('struct sockaddr_in', 'sin_family') == ffi.offsetof('struct sockaddr_in6', 'sin6_family'))
assert(ffi.offsetof('struct sockaddr_in', 'sin_port') == ffi.offsetof('struct sockaddr_in6', 'sin6_port'))

local SOL_SOCKET = ffi_state.defs.SOL_SOCKET
local SOCK_STREAM = ffi_state.defs.SOCK_STREAM
local SO_REUSEADDR = ffi_state.defs.SO_REUSEADDR
local SO_REUSEPORT = ffi_state.defs.SO_REUSEPORT
local SO_SNDTIMEO = ffi_state.defs.SO_SNDTIMEO
local SO_RCVTIMEO = ffi_state.defs.SO_RCVTIMEO
local SO_SNDBUF = ffi_state.defs.SO_SNDBUF
local SO_RCVBUF = ffi_state.defs.SO_RCVBUF
local AF_INET = ffi_state.defs.AF_INET

local F_SETFL = ffi_state.defs.F_SETFL
local F_GETFL = ffi_state.defs.F_GETFL
local O_NONBLOCK = ffi_state.defs.O_NONBLOCK

-- TODO : support PDP_ENDIAN (but which architecture uses this endian?)
local LITTLE_ENDIAN
if ffi.os == "OSX" then
	-- should check __DARWIN_BYTE_ORDER intead of BYTE_ORDER
	ffi_state = loader.load("endian.lua", {}, {
		"__DARWIN_BYTE_ORDER", "__DARWIN_LITTLE_ENDIAN", "__DARWIN_BIG_ENDIAN", "__DARWIN_PDP_ENDIAN"
	}, nil, [[
		#include <sys/types.h>
	]])
	assert(ffi_state.defs.__DARWIN_BYTE_ORDER ~= ffi_state.defs.__DARWIN_PDP_ENDIAN, "unsupported endian: PDP")
	LITTLE_ENDIAN = (ffi_state.defs.__DARWIN_BYTE_ORDER == ffi_state.defs.__DARWIN_LITTLE_ENDIAN)
elseif ffi.os == "Linux" then
	ffi_state = loader.load("endian.lua", {}, {
		"__BYTE_ORDER", "__LITTLE_ENDIAN", "__BIG_ENDIAN", "__PDP_ENDIAN"
	}, nil, [[
		#include <endian.h>
	]])
	assert(ffi_state.defs.__BYTE_ORDER ~= ffi_state.defs.__PDP_ENDIAN, "unsupported endian: PDP")
	LITTLE_ENDIAN = (ffi_state.defs.__BYTE_ORDER == ffi_state.defs.__LITTLE_ENDIAN)
end



-- returns true if litten endian arch, otherwise big endian. 
-- now this framework does not support pdp endian.
function _M.little_endian()
	return LITTLE_ENDIAN
end

--> htons/htonl/ntohs/ntohl 
--- borrow from http://svn.fonosfera.org/fon-ng/trunk/luci/libs/core/luasrc/ip.lua

--- Convert given short value to network byte order on little endian hosts
-- @param x	Unsigned integer value between 0x0000 and 0xFFFF
-- @return	Byte-swapped value
-- @see		htonl
-- @see		ntohs
function _M.htons(x)
	if LITTLE_ENDIAN then
		return bit.bor(
			bit.rshift( x, 8 ),
			bit.band( bit.lshift( x, 8 ), 0xFF00 )
		)
	else
		return x
	end
end

--- Convert given long value to network byte order on little endian hosts
-- @param x	Unsigned integer value between 0x00000000 and 0xFFFFFFFF
-- @return	Byte-swapped value
-- @see		htons
-- @see		ntohl
function _M.htonl(x)
	if LITTLE_ENDIAN then
		return bit.bor(
			bit.lshift( htons( bit.band( x, 0xFFFF ) ), 16 ),
			htons( bit.rshift( x, 16 ) )
		)
	else
		return x
	end
end

--- Convert given short value to host byte order on little endian hosts
-- @class	function
-- @name	ntohs
-- @param x	Unsigned integer value between 0x0000 and 0xFFFF
-- @return	Byte-swapped value
-- @see		htonl
-- @see		ntohs
_M.ntohs = _M.htons

--- Convert given short value to host byte order on little endian hosts
-- @class	function
-- @name	ntohl
-- @param x	Unsigned integer value between 0x00000000 and 0xFFFFFFFF
-- @return	Byte-swapped value
-- @see		htons
-- @see		ntohl
_M.ntohl = _M.htonl

-- load/store 2/4/8 byte from/to bytes array
local bytes_op = ffi.new('union luact_bytes_op')
function _M.get16(bytes)
	return bit.band( bytes[0], 
		bit.lshift(bytes[1], 8) 
	)
end
function _M.sget16(str)
	return bit.band( str:byte(1), 
		bit.lshift(str:byte(2), 8) 
	)
end
function _M.set16(bytes, v)
	bytes[0] = bit.band(v, 0xFF)
	bytes[1] = bit.rshift(bit.band(v, 0xFF00), 8)
end

function _M.get32(bytes)
	return bit.band( bytes[0], 
		bit.lshift(bytes[1], 8),  
		bit.lshift(bytes[2], 16), 
		bit.lshift(bytes[3], 24)
	)
end
function _M.sget32(str)
	return bit.band( str:byte(1), 
		bit.lshift(str:byte(2), 8),  
		bit.lshift(str:byte(3), 16), 
		bit.lshift(str:byte(4), 24)
	)
end
function _M.set32(bytes, v)
	bytes[0] = bit.band(v, 0xFF)
	bytes[1] = bit.rshift(bit.band(v, 0xFF00), 8)
	bytes[2] = bit.rshift(bit.band(v, 0xFF0000), 16)
	bytes[3] = bit.rshift(bit.band(v, 0xFF000000), 24)
end

function _M.get64(bytes)
	return bit.band( bytes[0], 
		bit.lshift(bytes[1], 8), 
		bit.lshift(bytes[2], 16), 
		bit.lshift(bytes[3], 24), 
		bit.lshift(bytes[4], 32), 
		bit.lshift(bytes[5], 40), 
		bit.lshift(bytes[6], 48), 
		bit.lshift(bytes[7], 56) 
	)
end
function _M.sget64(str)
	return bit.band( str:byte(1), 
		bit.lshift(str:byte(2), 8), 
		bit.lshift(str:byte(3), 16), 
		bit.lshift(str:byte(4), 24), 
		bit.lshift(str:byte(5), 32), 
		bit.lshift(str:byte(6), 40), 
		bit.lshift(str:byte(7), 48), 
		bit.lshift(str:byte(8), 56) 
	)
end
function _M.set64(bytes, v)
	bytes[0] = bit.band(v, 0xFF)
	bytes[1] = bit.rshift(bit.band(v, 0xFF00), 8)
	bytes[2] = bit.rshift(bit.band(v, 0xFF0000), 16)
	bytes[3] = bit.rshift(bit.band(v, 0xFF000000), 24)
	bytes[4] = bit.rshift(bit.band(v, 0xFF00000000), 32)
	bytes[5] = bit.rshift(bit.band(v, 0xFF0000000000), 40)
	bytes[6] = bit.rshift(bit.band(v, 0xFF000000000000), 48)
	bytes[7] = bit.rshift(bit.band(v, 0xFF00000000000000), 56)
end



--> misc network function
--> this (and 'default' below), and all the upvalue of module function
--> may seems functions not to be reentrant, but actually when luact runs with multithread mode, 
--> independent state is assigned to each thread. so its actually reentrant and thread safe.
local addrinfo_buffer = ffi.new('struct addrinfo * [1]')
local hint_buffer = ffi.new('struct addrinfo[1]')
function _M.inet_hostbyname(addr, addrp, socktype)
	local s,e,host,port = addr:find('([%w%.%_]+):([0-9]+)')
	if not s then 
		return -1
	end
	local sa = ffi.cast('struct sockaddr*', addrp)
	local ab, af, protocol, r
	socktype = socktype or SOCK_STREAM
	hint_buffer[0].ai_socktype = socktype
	if C.getaddrinfo(host, port, hint_buffer, addrinfo_buffer) < 0 then
		return -2
	end
	-- TODO : is it almost ok to use first entry of addrinfo?
	-- but create socket and try to bind/connect seems costly for checking
	ab = addrinfo_buffer[0]
	af = ab.ai_family
	protocol = ab.ai_protocol
	r = ab.ai_addrlen
	ffi.copy(addrp, ab.ai_addr, r)
	if addrinfo_buffer[0] ~= ffi.NULL then
		-- TODO : cache addrinfo_buffer[0] with addr as key
		C.freeaddrinfo(addrinfo_buffer[0])
		addrinfo_buffer[0] = ffi.NULL
	end
	return r, af, socktype, protocol
end
function _M.inet_namebyhost(addrp, dst, len)
	if not dst then
		dst = ffi.new('char[256]')
		len = 256
	end
	local sa = ffi.cast('struct sockaddr_in*', addrp)
	local p = C.inet_ntop(sa.sin_family, addrp, dst, len)
	if p == ffi.NULL then
		return "invalid addr data"
	else
		return ffi.string(dst)..":"..tostring(_M.ntohs(sa.sin_port))
	end
end

local default = memory.alloc_fill_typed('luact_sockopt_t')
function _M.setsockopt(fd, opts)
	opts = opts or default
	if not opts.blocking then
		local f = C.fcntl(fd, F_GETFL, 0) 
		if f < 0 then
			print(ERROR,SOCKOPT,("fcntl fail (get flag) errno=%d"):format(ffi.errno()));
			return -6
		end
		-- fcntl declaration is int fcntl(int, int, ...), 
		-- that means third argument type is vararg, which less converted than usual ffi function call 
		-- (eg. lua-number to double to int), so you need to convert to int by you
		if C.fcntl(fd, F_SETFL, ffi.new('int', bit.bor(f, O_NONBLOCK))) < 0 then
			print(ERROR,SOCKOPT,("fcntl fail (set nonblock) errno=%d"):format(ffi.errno()));
			return -1
		end
		-- print('fd = ' .. fd, 'set as non block('..C.fcntl(fd, F_GETFL)..')')
	end
	if opts.timeout > 0 then
		assert(false, "i dont set timeout for opts!!!!"..opts.timeout)
		local timeout = util.sec2timeval(tonumber(opts.timeout))
		if C.setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, timeout, ffi.sizeof('struct timeval')) < 0 then
			print(ERROR,SOCKOPT,("setsockopt (sndtimeo) errno=%d"):format(ffi.errno()));
			return -2
		end
		if C.setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, timeout, ffi.sizeof('struct timeval')) < 0 then
			print(ERROR,SOCKOPT,("setsockopt (rcvtimeo) errno=%d"):format(ffi.errno()));
			return -3
		end
	end
	if opts.wblen.data > 0 then
		print(("%d, set wblen to %u\n"):format(fd, tonumber(opts.wblen)));
		if C.setsockopt(fd, SOL_SOCKET, SO_SNDBUF, opts.wblen.p, ffi.sizeof(opts.wblen.p)) < 0 then
			print(ERROR,SOCKOPT,"setsockopt (sndbuf) errno=%d", errno);
			return -4
		end
	end
	if opts.rblen.data > 0 then
		print(("%d, set rblen to %u\n"):format(fd, tonumber(opts.wblen.data)));
		if C.setsockopt(fd, SOL_SOCKET, SO_RCVBUF, opts.rblen.p, ffi.sizeof(opts.rblen.p)) < 0 then
			print(ERROR,SOCKOPT,"setsockopt (rcvbuf) errno=%d", errno);
			return -5
		end
	end
	return 0
end

function _M.set_reuse_addr(fd, reuse)
	reuse = ffi.new('int[1]', {reuse and 1 or 0})
	if C.setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, reuse, ffi.sizeof(reuse)) < 0 then
		error('fail to setsockopt:reuseaddr:'..ffi.errno())
	end
	print('reuse addr:', SO_REUSEPORT, _M.port_reusable())
	if _M.port_reusable() then
		print('add reuse port')
		if C.setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, reuse, ffi.sizeof(reuse)) < 0 then
			error('fail to setsockopt:reuseport:'..ffi.errno())
		end
	end
end

function _M.port_reusable()
	return SO_REUSEPORT
end

function _M.create_stream(addr, opts, addrinfo)
	local r, af = _M.inet_hostbyname(addr, addrinfo.addrp)
	if r < 0 then
		print('invalid address:', addr)
		return nil
	end
	addrinfo.alen[0] = r
	local fd = C.socket(af, SOCK_STREAM, 0)
	if fd < 0 then
		print('fail to create socket:'..ffi.errno())
		return nil
	end
	if _M.setsockopt(fd, opts) < 0 then
		print('fail to set socket options:'..ffi.errno())
		C.close(fd)
		return nil
	end
	return fd
end

function _M.create_dgram(addr, opts, addrinfo)
	local r, af = _M.inet_hostbyname(addr, addrinfo.addrp, SOCK_DGRAM)
	if r < 0 then
		print('invalid address:', addr)
		return nil
	end
	addrinfo.alen[0] = r
	local fd = C.socket(af, SOCK_DGRAM, 0)
	if fd < 0 then
		print('fail to create socket:'..ffi.errno())
		return nil
	end
	if _M.setsockopt(fd, opts) < 0 then
		print('fail to set socket options:'..ffi.errno())
		C.close(fd)
		return nil
	end
	return fd
end

function _M.create_mcast(addr, opts, addrinfo)
	-- TODO : create multicast
end

return _M
