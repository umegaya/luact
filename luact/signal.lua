local loader = require 'luact.loader'
local ffi = require 'ffiex'
local memory = require 'luact.memory'
local util = require 'luact.util'

local C = ffi.C
local _M = {}
local ffi_state

loader.add_lazy_init(_M)

local SIG_IGN

function _M.init_cdef()
	ffi_state = loader.load("signal.lua", {
		"signal", "sigaction", "sigemptyset", "sig_t", 
	}, {
		"SIGHUP", "SIGPIPE", "SIGKILL", "SIGALRM", "SIGSEGV", 
		regex = {
			"^SIG%w+"
		}
	}, nil, [[
		#include <signal.h>
	]])

	--> that is really disappointing, but macro SIG_IGN is now cannot processed correctly. 
	SIG_IGN = ffi.cast("sig_t", 1)
end

function _M.ignore(signo)
	signo = (type(signo) == 'number' and signo or _M[signo])
	C.signal(signo, SIG_IGN)
end

function _M.signal(signo, handler)
	signo = (type(signo) == 'number' and signo or _M[signo])
	local sa = memory.managed_alloc_typed('struct sigaction[1]')
	local sset = memory.managed_alloc_typed('sigset_t[1]')
	sa[0].sa_handler = handler
	sa[0].sa_flags = SA_RESTART
	if C.sigemptyset(sset) ~= 0 then
		return false
	end
	sa[0].sa_mask = sset[0]
	if C.sigaction(signo, sa, 0) ~= 0 then
		return false
	end
	return true
end

return setmetatable(_M, {
	__index = function (t, k)
		local v = ffi_state.defs[k]
		assert(v, "no signal definition:"..k)
		rawset(t, k, v)
		return v
	end
})
