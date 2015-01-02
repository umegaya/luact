--[[
gossip propagates nodes information to cluster
it at least contains: 
	1. node address (ipv4)
	2. hardware information
		# of core
		physical memory size
		disk information (size, ssd/hdd)
	3. luajit VM state (contains luact stat itself) 
		for each thread
]]--

local _M = {}

return _M
