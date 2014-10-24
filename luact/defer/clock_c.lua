local pulpo = require 'pulpo.init'
local _M = (require 'pulpo.package').module('luact.defer.clock_c')
local clock = pulpo.evloop.clock.new(0.01, 10)

function _M.sleep(sec)
	clock:sleep(sec)
end
-- returns clock in sec (as double precision)
function _M.get()
	return pulpo.util.clock()
end

return _M
