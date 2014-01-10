local _M = {}

_M.n_cpu = function ()
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

-- TODO : how we import gcc's __sync_XXXX into luaJIT?
_M.sync_fetch_add = function (n, add)
	n[0] = n[0] + add
	return n[0]
end
_M.sync_cas = function (value, compare, willbe)
	if value[0] == compare then
		local tmp = value[0]
		value[0] = willbe
		return tmp
	end
end

return _M
