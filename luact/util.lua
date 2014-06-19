local ffi = require 'ffiex'
local loader = require 'luact.loader'

local C = ffi.C
local _M = {}

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
	return t1
end

--> ffi related utils
local C = ffi.C
local RLIMIT_NOFILE, RLIMIT_CORE
loader.add_lazy_initializer(function ()
	local ffi_state = loader.load('util.lua', {
		"getrlimit", "setrlimit", "struct timespec", "struct timeval", "nanosleep",
	}, {
		"RLIMIT_NOFILE",
		"RLIMIT_CORE",
	}, nil, [[
		#include <sys/time.h> 
		#include <sys/resource.h>
	]])

	RLIMIT_CORE = ffi_state.defs.RLIMIT_CORE
	RLIMIT_NOFILE = ffi_state.defs.RLIMIT_NOFILE

	_M.req,_M.rem = ffi.new('struct timespec[1]'), ffi.new('struct timespec[1]')
end)

function _M.maxfd(set_to)
	if set_to then
		--> set max_fd to *set_to*
		C.setrlimit(RLIMIT_NOFILE, ffi.new('struct rlimit', {set_to, set_to}))
		return set_to
	else
		--> returns current max_fd
		return C.getrlimit(RLIMIT_NOFILE)
	end
end

function _M.maxconn(set_to)
--[[ 
	if os somaxconn is less than TCP_LISTEN_BACKLOG, increase value by
	linux:	sudo /sbin/sysctl -w net.core.somaxconn=NBR_TCP_LISTEN_BACKLOG
			(and sudo /sbin/sysctl -w net.core.netdev_max_backlog=3000)
	osx:	sudo sysctl -w kern.ipc.somaxconn=NBR_TCP_LISTEN_BACKLOG
	(from http://docs.codehaus.org/display/JETTY/HighLoadServers)
]]
	if ffi.os == "Linux" then
	io.popen(('sudo /sbin/sysctl -w net.core.somaxconn=%d'):format(set_to))
	elseif ffi.os == "OSX" then
	io.popen(('sudo sysctl -w kern.ipc.somaxconn=%d'):format(set_to))
	end
	return set_to
end

function _M.setsockbuf(rb, wb)
	--[[ TODO: change rbuf/wbuf max /*
	* 	you may change your system setting for large wb, rb. 
	*	eg)
	*	macosx: sysctl -w kern.ipc.maxsockbuf=8000000 & 
	*			sysctl -w net.inet.tcp.sendspace=4000000 sysctl -w net.inet.tcp.recvspace=4000000 
	*	linux:	/proc/sys/net/core/rmem_max       - maximum receive window
    *			/proc/sys/net/core/wmem_max       - maximum send window
    *			(but for linux, below page will not recommend manual tuning because default it set to 4MB)
	*	see http://www.psc.edu/index.php/networking/641-tcp-tune for detail
	*/]]
	return rb, wb
end

function _M.sec2timespec(sec, ts)
	ts = ts or ffi.new('struct timespec[1]')
	local round = math.floor(sec)
	ts[0].tv_sec = round
	ts[0].tv_nsec = math.floor((sec - round) * (1000 * 1000 * 1000))
	return ts
end
function _M.sec2timeval(sec, ts)
	ts = ts or ffi.new('struct timeval[1]')
	local round = math.floor(sec)
	ts[0].tv_sec = round
	ts[0].tv_usec = math.floor((sec - round) * (1000 * 1000))
	return ts
end
-- nanosleep
function _M.sleep(sec)
	-- convert to nsec
	local req, rem = _M.req, _M.rem
	_M.sec2timespec(sec, _M.req)
	while C.nanosleep(req, rem) ~= 0 do
		local tmp = req
		req = rem
		rem = tmp
	end
end

return _M
