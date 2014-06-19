local thread = require 'luact.thread'
local poller = require 'luact.poller'

local _M = {}

function _M.initialize(opts)
	thread.initialize(opts)
	poller.initialize(opts)
end

