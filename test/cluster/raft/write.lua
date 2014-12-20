local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_local_cluster(5, 3, tools.new_fsm, function (arbiter, thread_id)
	local clock = require 'luact.clock'
	local event = require 'pulpo.event'
	logger.info('---------------------- start cluster ---------------------------')
	if thread_id == 3 then
		local evs = {}
		for i=1,10 do
			table.insert(evs, arbiter:async_propose({{i, i * 11}}))
		end
		local results = event.join(clock.alarm(5), unpack(evs))
		assert(results[#results][1] == 'ontime', "takes too long time")
	end
	local r = arbiter:probe(function (rft)
		local fsm = rft.state.fsm
		for i=1,10 do
			if fsm[i] ~= (i * 11) then
				return r
			end
		end
	end)
	assert(not r, "proposed log should be applied to fsm:"..tostring(r))
	logger.info('success')
	clock.sleep(thread_id == 3 and 5 or 1)
end)

