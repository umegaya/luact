local conhash = require 'luact.ch'
local conn = require 'luact.conn'

local _M = {}

_M.table = setmetatable({}, {
	__index = function (t, k)
	end,
	__newindex = function (t, k, v)
	end,
})

function _M.initialize(cmdl_args)
end

return _M
