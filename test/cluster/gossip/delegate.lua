local luact = require 'luact.init'
local tools = require 'test.tools.cluster'

tools.start_luact(4, nil, function ()
	local ffi = require 'ffiex.init'

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
	local gossiper = luact.root_actor.gossiper(8008, {
		delegate = function ()
			local ffi = require 'ffiex.init'
			local pulpo = require 'pulpo.init'
			local r = setmetatable({ count = 0, event_stack = {}, }, {
				__index = {
					user_state = function (self)
						self.count = self.count + 1
						return ffi.new('int[1]', pulpo.thread_id * 1000 + self.count), ffi.sizeof('int')
					end,
					memberlist_event = function (self, t, ...)
						table.insert(self.event_stack, {t, ...})
					end,
				}
			})
			logger.report('delegate created', r)
			return r
		end,
	})
	assert(gossiper:wait_bootstrap(5), "initialization should not be timeout")
	clock.sleep(1.0)
	local ok, r = gossiper:probe(function (g)
		local ffi = require 'ffiex.init'
		assert(g.delegate.count == 1, "user state should called once")
		-- logger.warn('gnodes', #g.nodes)
		for i=1,#g.nodes do
			local n = g.nodes[i]
			logger.warn('user state', ffi.cast('int*', n.user_state)[0], n.thread_id)
			assert((1000 * n.thread_id + 1) == ffi.cast('int*', n.user_state)[0])
		end
	end)
	assert(ok, r)
	p:wait(1)

	if pulpo.thread_id == 1 then
		gossiper:broadcast_user_state()
	else
		local ok, r = gossiper:probe(function (g)
			local ffi = require 'ffiex.init'
			local clock = require 'luact.clock'
			local event = require 'pulpo.event'
			local count = 0
			while true do
				local es = g.delegate.event_stack
				local found
				for i=1,#es do
					if es[i][1] == 'change' then
						found = true
						break
					end
				end
				if found then break end
				logger.warn('waiting change...', count)
				clock.sleep(0.5)
				count = count + 1
				if count > 10 then
					assert(false, "event notification timeout")
				end
			end
			for i=1,#g.nodes do
				local n = g.nodes[i]
			logger.warn('user state', ffi.cast('int*', n.user_state)[0], n.thread_id)
				if n.thread_id ~= 1 then
					assert((1000 * n.thread_id + 1) == ffi.cast('int*', n.user_state)[0])
				else
					assert((1000 * n.thread_id + 2) == ffi.cast('int*', n.user_state)[0])
				end
			end
		end)
		assert(ok, r)
	end
	p:wait(2)
end)

return true