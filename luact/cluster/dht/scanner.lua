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

-- maintain_replica maintain number of replica which handles commands for the range *self*. 
-- first maintain_replica try to increase # of available replica, then initialize raft's replica_set.
-- NOTE : this task initiated by calling range:start_raft_group, and this function create raft actor in this range, which will be leader.
-- so we can assure that leader node already added as range replica. 
local function maintain_replica(self, gossiper, opts)
	local replica_added = {}
	if self.replica_available < self.required_replica then
		logger.info('scanner', 'maintain_replica phase1', self)
		-- initial ranges should be processed in here.
		local n_require = self.required_replica - self.replica_available
		local list = gossip.nodelist(opts.gossip_port)
		for i=1, #list do
			local n = list[i]
			if not self:find_replica_by(n, opts.allow_same_node) then
				local ok, rep = pcall(n.arbiter, n, self:arbiter_id(), _M.range_fsm_factory, nil, self)
				if ok then
					table.insert(replica_added, rep)
				else
					logger.warn('error get arbiter', n, rep)
				end
				if #replica_added >= n_require then
					goto add_replica_set
				end
			end
		end
	else
		--[[ split range should be processed here
		-- because it copies raft's replica set from original range but no raft replica set exists.
		if not self:is_replica_synchorinized_with_raft() then
			local replica_set = self:raft_body():replica_set()
			logger.info('scanner', 'maintain_replica phase2', self)
			local n_require = self.replica_available - #replica_set
			-- if there is a replica which not added as raft replica set.
			for i=0, self.replica_available-1 do -- self is cdata
				local r = self.replicas[i]
				local mid, tid, found = uuid.addr(r), uuid.thread_id(r)
				for j=1, #replica_set do
					local r2 = replica_set[j]
					local mid2, tid2 = uuid.addr(r2), uuid.thread_id(r2)
					if mid == mid2 and tid == tid2 then
						found = true
						break
					end
				end
				if not found then
					-- replica in range is not found in raft's replica set.
					local n = actor.root_of(mid, tid)
					local ok, rep = pcall(n.arbiter, self:arbiter_id(), _M.range_fsm_factory, nil, self)
					if ok then
						table.insert(replica_added, rep)
					else
						logger.warn('error get arbiter', n, rep)
					end
					if #replica_added >= n_require then
						goto add_replica_set
					end
				end
			end
		end
		]]--
	end
::add_replica_set::
	if #replica_added > 0 then
		local rft = self:raft_body()
		-- this eventually calls range:change_replica_set, which changes this range data.
		local ok, r = pcall(rft.add_replica_set, rft, replica_added)
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
	logger.info('scanner start', tostring(ffi.cast('luact_uuid_t *', key)))
end

function _M.stop(rng)
	local ths = scan_threads[rng:arbiter_id()]
	scan_threads[rng:arbiter_id()] = nil
	for i=1,#ths do
		tentacle.cancel(ths[i])
	end
end


return _M
