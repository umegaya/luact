local ffi = require 'ffiex'
local loader = require 'luact.loader'

local C = ffi.C
local _M = {}

loader.add_lazy_init(_M)

--> add NULL symbol
ffi.NULL = ffi.new('void*')

--> non-ffi related util
function _M.n_cpu()
	local c = 0
	-- FIXME: I dont know about windows... just use only 1 core.
	if jit.os == 'Windows' then return 1 end
	if jit.os == 'OSX' then
		local ok, r = pcall(io.popen, 'sysctl -a machdep.cpu | grep thread_count')
		if not ok then return 1 end
		c = 1
		for l in r:lines() do 
			c = l:gsub("machdep.cpu.thread_count:%s?(%d+)", "%1")
		end
	else
		local ok, r = pcall(io.popen, 'cat /proc/cpuinfo | grep processor')
		if not ok then return 1 end
		for l in r:lines() do c = c + 1 end
		r:close()
	end
	return tonumber(c)
end

function _M.mkdir(path)
	-- TODO : windows?
	os.execute(('mkdir -p %s'):format(path))
end

function _M.rmdir(path)
	-- TODO : windows?
	os.execute(('rm -rf %s'):format(path))
end

function _M.merge_table(t1, t2)
	for k,v in pairs(t2) do
		t1[k] = v
	end
end

--> ffi related utils
local C = ffi.C
function _M.init_cdef()
	loader.load('util.lua', {
		"getrlimit", 
		"setrlimit",
	}, {
		"RLIMIT_NOFILE",
		"RLIMIT_CORE",
	}, nil, [[
		#include <sys/time.h> 
		#include <sys/resource.h>
	]])
end

function _M.maxfd(set_to)
	if set_to then
		--> set max_fd to *set_to*
		C.setrlimit(ffi.defs.RLIMIT_NOFILE, ffi.new('struct rlimit', {set_to, set_to}))
		return set_to
	else
		--> returns current max_fd
		return C.getrlimit(ffi.defs.RLIMIT_NOFILE)
	end
end

function _M.sec2timespec(sec, ts)
	local round = math.floor(sec)
	ts[0].tv_sec = round
	ts[0].tv_nsec = math.floor((sec - round) * (1000 * 1000 * 1000))
end

local AF_INET = ffi.defs.AF_INET
function _M.inet_hostbyname(addr, addrp, len)
	local s,e,host,port = addr:find('([%w%.%_]+):([0-9]+)')
	if not s then error('invalid address format:'..addr) end
	local sa = ffi.cast('struct sockaddr_in*', addrp)
	local hp = C.gethostbyname(host)
	sa.sin_family = AF_INET
	sa.sin_addr.s_addr = C.htonl(hp.h_addr_list[0])
	sa.sin_port = C.htons(port)
	return ffi.sizeof('struct sockaddr_in')
end

return _M
