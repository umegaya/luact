local thread = require 'pulpo.thread'

local _M = {}
local clock

thread.add_initializer(function (loader, shmem)
	clock = pulpo.evloop.clock.new(0.05, 10)
end)

function _M.sleep(sec)
	clock:sleep(sec)
end

return _M
