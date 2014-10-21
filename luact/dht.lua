local conhash = require 'luact.ch'
local conn -- luact.conn

local _M = {}

function _M.initialize(cmdl_args)
	-- need to register itself to dht cluster
	conn = require 'luact.conn'
end

_M.table = setmetatable({}, {
	__index = function (t, k)
	end,
	__newindex = function (t, k, v)
	end,
})

return _M
