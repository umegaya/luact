local luact = require 'luact.init'
local uuid = require 'luact.uuid'
local clock = require 'luact.clock'
local serde = require 'luact.serde'
local serde_common = require 'luact.serde.common'
local router = require 'luact.router'
local actor = require 'luact.actor'

local pulpo = require 'pulpo.init'
local event = require 'pulpo.event'
local util = require 'pulpo.util'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local tentacle = require 'pulpo.tentacle'
local lamport = require 'pulpo.lamport'
local fs = require 'pulpo.fs'

local raft = require 'luact.cluster.raft'
local gossip = require 'luact.cluster.gossip'
local key = require 'luact.cluster.dht.key'
local cache = require 'luact.cluster.dht.cache'
local cmd = require 'luact.cluster.dht.cmd'

local _M = {}

local scan_threads = {}


function _M.range_fsm_factory(rng)
	-- because this code execute at different thread/node, cannot rely on upvalues
	local rm = (require 'luact.cluster.dht.range').get_manager()
	return rm:create_fsm_for_arbiter(rng)
end

local function maintain_replica(self, gossiper, opts)
	if self.replica_available < self.n_replica then
		logger.info('scanner', 'maintain_replica', self)
		local n_require = self.n_replica - self.replica_available
		local list = gossip.nodelist(opts.gossip_port)
		local replica_set = {}
		for i=1, #list do
			local n = list[i]
			if not self:find_replica_by(n, opts.allow_same_node) then
				-- create new arbitor for this range and add it as replica set
				local ok, rep = pcall(n.arbiter, n, self:arbiter_id(), _M.range_fsm_factory, nil, self)
				if ok then
					table.insert(replica_set, rep)
					if #replica_set >= n_require then
						break
					end
				else
					logger.warn('error get arbiter', n, rep)
				end
			end
		end
		local ok, r = pcall(self.replicas[0].add_replica_set, self.replicas[0], replica_set)
		if ok then
			-- broadcast
			gossiper:broadcast(cmd.gossip.replica_change(self), cmd.GOSSIP_REPLICA_CHANGE)
		end
	end
end

local function collect_garbage(self, gossiper, opts)
end

local function scanner(func, interval, rng, range_manager)
	return tentacle(function (fn, intv, r, rm)
		local opts, gossiper = rm.opts, rm.gossiper
		while true do
			fn(r, gossiper, opts)
			luact.clock.sleep(intv)
		end
	end, func, interval, rng, range_manager)
end

function _M.start(rng, range_manager)
	if not rng:belongs_to() then
		exception.raise('invalid', 'non-leader of this range should not run scanner', rng)
	end
	--[[ do following steps periodically (should run only with leader)
		1. check number of replica, if too short, supply new node from nodelist
		2. for each key in range, remove the value which has too old version
	]]
	local key = rng:arbiter_id()
	if scan_threads[key] then
		_M.stop(rng)
	end
	local opts = range_manager.opts
	scan_threads[key] = {
		scanner(maintain_replica, opts.replica_maintain_interval, rng, range_manager),
		scanner(collect_garbage, opts.collect_garbage_interval, rng, range_manager),
	}
	logger.info('scanner start', ('%q'):format(key))
end

function _M.stop(rng)
	local ths = scan_threads[rng:arbiter_id()]
	scan_threads[rng:arbiter_id()] = nil
	for i=1,#ths do
		tentacle.cancel(ths[i])
	end
end


return _M
