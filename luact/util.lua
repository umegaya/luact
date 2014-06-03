local _M = {}
local ffi = require 'ffiex'

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

-- atomic builtins
--[[
local synclib = ffi.csrc("synclib", [[
extern int fetch_add(int *p, int add) {
	return __sync_fetch_and_add(p, add);
}
extern int cas(int *v, int compare, int willbe) {
	return __sync_val_compare_and_swap(v, compare, willbe);
}
, {
	cc = "gcc", 
	extra = {"-O2", "-Wall"},
})

-- n:int *, add:int
function _M.sync_fetch_add(n, add)
	return synclib.fetch_add(n, add)
end
-- n:int *, add:int
function _M.sync_cas(value, compare, willbe)
	return synclib.cas(value, compare, willbe);
end
]]--
return _M
