local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_luact(4, nil, function ()
	local memory = require 'pulpo.memory'
	local pulpo = require 'pulpo.init'
	local event = require 'pulpo.event'
	local tentacle = require 'pulpo.tentacle'
	
	local luact = require 'luact.init'
	local uuid = require 'luact.uuid'
	local actor = require 'luact.actor'
	local clock = require 'luact.clock'
	local gossip = require 'luact.cluster.gossip'
	local tools = require 'test.tools.cluster'

	local p = tools.create_latch('checker', 4)
	local gossiper = luact.root_actor.gossiper(8008)
	
	-- test for start up
	local nodes = {}
	local ev = gossip.event(8008)
	assert(ev, "gossip event should be initialized")
	local t = tentacle(function (e)
		while true do
			local tp, obj, node = event.wait(false, e)
			logger.info('event', tp, node)
			if tp == 'join' then
				assert(ffi.typeof(node) == ffi.typeof('struct luact_gossip_node*'))
				table.insert(nodes, node)
			end
		end
	end, ev)
	assert(gossiper:wait_bootstrap(5), "initialization should not be timeout")
	clock.sleep(1.0) -- wait for state sync
	p:wait(1)
	-- stop tentacle for gathering node information
	tentacle.cancel(t)
	if pulpo.thread_id == 1 then
		assert(#nodes == 3, "all nodes should be propagated")
		for _, n in ipairs(nodes) do
			assert(n.thread_id ~= pulpo.thread_id, "this node itself should not be notified through event")
			assert(1 <= n.thread_id and n.thread_id <= 4, "node data should be correct")
			assert(n.machine_id == uuid.node_address, "node data should be correct")
		end
	else
		for _, n in ipairs(nodes) do
			assert(n.thread_id ~= pulpo.thread_id, "this node itself should not be notified through event")
			assert(1 < n.thread_id and n.thread_id <= 4, "node data should be correct")
			assert(n.machine_id == uuid.node_address, "node data should be correct")
		end
	end
	p:wait(2)
	-- test for leaving
	if pulpo.thread_id == 4 then
		assert(gossiper:leave(5), "should be able to leave without timed out")
	end
	p:wait(3)
	clock.sleep(gossiper:suspicion_timeout() + 1) -- + 1 for ensure timeout 
	if pulpo.thread_id ~= 4 then
		-- check left node is actually removed from nodelist
		local ok, r = gossiper:probe(function (g)
			assert(#g.nodes == 3, "left node should be deleted:"..#g.nodes)
			for _, n in ipairs(g.nodes) do
				assert(1 <= n.thread_id and n.thread_id <= 3, "node data should be correct")
			end
		end)
		assert(ok, "invalid gossiper state:"..tostring(r))
	end
	p:wait(4)

	-- test for user broadcast
	if pulpo.thread_id == 4 then
		local ok, r = pcall(gossiper.restart, gossiper)
		assert((not ok) and r:is('actor_error'))
		while true do
			ok, r = pcall(gossiper.wait_bootstrap, gossiper, 5)
			if ok then
				break
			elseif not r:is('actor_temporary_fail') then
				assert(false, "no other error of actor_temporary_fail is permissible")
			end
			clock.sleep(0.1)
		end
	end
	p:wait(5)
	local ok, r = gossiper:probe(function (g)
		local clock = require 'luact.clock'
		local event = require 'pulpo.event'
		local count = 0
		while #g.nodes < 4 do
			local tp = event.wait(nil, g.event, clock.alarm(0.5))
			if tp == 'join' then
				break
			end
			count = count + 1
			if count > 20 then
				assert(false, "node addition timeout")
			end
		end
		assert(#g.nodes == 4, "left node should be added")
		for _, n in ipairs(g.nodes) do
			assert(1 <= n.thread_id and n.thread_id <= 4, "node data should be correct")
		end
	end)
	assert(ok, "invalid gossiper state:"..tostring(r))
	p:wait(6)
	-- check user defined broadcast message processed correctly
	local msg = "hey! i am resurrect :D"
	if pulpo.thread_id == 4 then
		clock.sleep(1.0)
		gossiper:broadcast(msg)
	else
		while true do
			local tp, obj, buf, len = event.wait(false, clock.alarm(10), ev)
			if tp == 'user' then
				logger.info('user msg:', buf, len, ffi.string(buf, len))
				assert(ffi.string(buf, len) == msg, "correct broadcast msg should be received")
				break
			elseif tp == 'read' then
				assert(false, "broadcast msg should arrive within timeout")
			end
		end
	end
	p:wait(7)
end)



return true
