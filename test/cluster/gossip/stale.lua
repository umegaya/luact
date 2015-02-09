local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_luact(4, nil, function ()
	local ffi = require 'ffiex.init'

	local memory = require 'pulpo.memory'
	local pulpo = require 'pulpo.init'
	local event = require 'pulpo.event'
	local tentacle = require 'pulpo.tentacle'
	local util = require 'pulpo.util'
	
	local luact = require 'luact.init'
	local uuid = require 'luact.uuid'
	local actor = require 'luact.actor'
	local clock = require 'luact.clock'
	local gossip = require 'luact.cluster.gossip'
	local tools = require 'test.tools.cluster'

	local p = tools.create_latch('checker', 4)

	local gossiper = luact.root_actor.gossiper(8008, {
		exchange_interval = 4.0,
	})
	assert(gossiper:wait_bootstrap(5), "initialization should not be timeout")
	clock.sleep(2.0)
	assert(gossiper:probe(function (g)
		while #g.nodes < 4 do
			clock.sleep(0.1)
		end
	end))
	p:wait(1)

	-- stale thread 3
	if pulpo.thread_id == 3 then
		logger.report('start staling')
		util.sleep(10.0) -- hard sleep
		logger.report('end staling')
	else
		assert(gossiper:timed_probe(15, function (g)
			local clock = require 'luact.clock'
			local event = require 'pulpo.event'
			local count = 0
			local n
			for i=1,4 do
				if g.nodes[i].thread_id == 3 then
					n = g.nodes[i]
				end
			end
			assert(n)
			if n:is_dead() then
				logger.warn('staling thread already marked as dead')
			else
				local change_count = 0
				while true do
					local tp = event.wait(nil, g.event, clock.alarm(0.5))
					if tp == 'leave' then
						assert(n:is_dead())
						break
					end
					if tp == 'change' then
						change_count = change_count + 1
						if change_count == 1 then
							assert(n:is_suspicious())
						elseif change_count == 2 then
							-- node 3 wakeup early and refute suspect message
							assert(n:is_alive())
							break
						end
					end
					count = count + 1
					if count > 30 then
						assert(false, "node leaving timeout")
					end
				end
			end
			assert(#g.nodes == 4)
			for i=1,4 do
				if g.nodes[i].thread_id ~= 3 then
					assert(g.nodes[i]:is_alive())
				else
					assert(g.nodes[i]:is_dead())
				end
			end
		end))
	end
	p:wait(2)

	-- after go back from stale, it will get back alive node list by periodical exchange_with
	clock.sleep(5.0)
	assert(gossiper:probe(function (g)
		assert(#g.nodes == 4)
	end))
	p:wait(3)
end)

return true