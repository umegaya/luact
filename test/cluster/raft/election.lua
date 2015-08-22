local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_luact(1, nil, function ()
	local luact = require 'luact.init'
	local raft = require 'luact.cluster.raft'
	local actor = require 'luact.actor'
	local clock = require 'luact.clock'
	local uuid = require 'luact.uuid'
	local pulpo = require 'pulpo.init'
	local tools = require 'test.tools.cluster'
	
	local arb = actor.root_of(nil, pulpo.thread_id).arbiter('test_group', tools.new_fsm, {initial_node = true}, pulpo.thread_id)
	clock.sleep(3)
	local r = raft._find_body('test_group')
	assert(uuid.valid(r:leader()), "if initial_node, leader should be elected with single node")
end)



return true

