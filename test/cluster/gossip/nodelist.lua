local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_luact(1, nil, function ()
	local pulpo = require 'pulpo.init'
	local uuid = require 'luact.uuid'
	local nodelist = require 'luact.cluster.gossip.nodelist'
	local n = nodelist.new(8008)
	n:add_self()
	assert(#n == 1, "1 node should be added")
	local self = n:self()
	assert(self.thread_id == pulpo.thread_id and self.machine_id == uuid.node_address, "node which represent this node should be added")
	-- mock
	function nodelist.gossiper_from(mid, tid)
		-- print('gfrom', mid, tid)
		return self.actor
	end
	local _1 = n:add_by_hostname('1.1.1.1:1111', 2)
	local _2 = n:add_by_hostname('2.2.2.2:2222', 3)
	local _3 = n:add_by_hostname('3.3.3.3:3333', 4)
	local _4 = n:add_by_hostname('4.4.4.4:4444', 5)
	assert(#n == 5, "node added by hostname should be added correctly")
	assert(_1 == n:add_by_hostname('1.1.1.1:1111', 2))
	assert(#n == 5, "same node should not be added again")
	local _1_1 = n:add_by_hostname('1.1.1.1:1111', 1)
	assert(#n == 6, "node which has different thread_id should be added again")
	n:remove(_1_1)
	assert(#n == 5, "node should be removed correctly")
	assert(self == n:find_by_nodedata({
		thread_id = pulpo.thread_id,
		machine_id = uuid.node_address,
	}))
	local rnd = n:k_random(3)
	assert(#rnd == 3, "number of node returned is same as the parameter given to k_random")
	for _, nd in ipairs(rnd) do
		local t = nd.thread_id
		assert(t <= 5 and t >= 1)
		if t ~= 1 then
			t = t - 1
			assert(tostring(nd.addr) == ('%d.%d.%d.%d:8008'):format(t,t,t,t), "for dummy node, correct value should be assigned")
		end
	end
end)



return true

