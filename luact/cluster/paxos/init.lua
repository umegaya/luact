local luact = require 'luact.init'
local uuid = require 'luact.uuid'
local clock = require 'luact.clock'

local event = require 'pulpo.event'
local util = require 'pulpo.util'
local nevent = util -- for GO style select
local socket = require 'pulpo.socket'
local tentacle = require 'pulpo.tentacle'
local exception = require 'pulpo.exception'

local _M = {}
local paxosmap = {}

exception.define('paxos')

-- tickers
local ticker = clock.ticker(1.0)


-- paxos object methods
local paxos_index = {}
local paxos_mt = {
	paxos_index = paxos_index
}
function paxos_index:init()
end
function paxos_index:fin()
	self.closer:emit('close')
end
function paxos_index:__actor_destroy__()
	self:fin()
end
function paxos_index:propose_proc(log)
	-- self.quorum, self.nodes might change during execution
	-- TODO : should use most low-latent k node?
	local q, nodes = self.quorum, util.random_k_from(self.nodes, self.quorum) 
	if #nodes < q then
		exception.raise('paxos', '# of quorum short', q, #nodes)
	end
	local ballot
	local prepares, commits = {}, {}
	local selector = {
		[ticker] = function (t)
			t.n_tick = t.n_tick + 1
			return t.n_tick >= _M.timeout_sec_count-- break select loop
		end,
		[prepares] = function (t, ...)
			local result, ballot, prop = ...
			if t.max_ballot < ballot then
				t.max_ballot = ballot
				t.proposal = prop
			end
			if result then
				t.n_success = t.n_success + 1
			else
				t.error = true
				return true -- error break
			end
		end,
		[commits] = function (t, ...)
			local result = ...
			if result then
				t.n_success = t.n_success + 1
			else
			end
		end,
		[self.closer] = function (t)
			exception.exception('report', 'graceful shutdown')
		end,
	}
::restart::	
	-------------------------
	-- prepare
	ballot = self:new_ballot_number(self.last_ballot)
	for _, node in ipairs(nodes) do
		local f = node:async_prepare(ballot)
		table.insert(prepares, f:rawevent())
	end
	-- gathering prepare result and decide proposal value
	selector.proposal = logger
	selector.n_tick, selector.max_ballot, selector.n_success = 0, ballot, 0
	nevent.select(selector)
	if selector.error then
		ballot = self.new_ballot_number(selector.max_ballot)
		goto restart
	end
	if selector.n_success < q then
		logger.warn('paxos', 'prepare', 'cannot get quorum', selector.nresp, q)
		ballot = self.new_ballot_number(selector.max_ballot)
		-- TODO : sleep?
		goto restart
	end

	-------------------------
	-- commit
	for _, node in ipairs(nodes) do
		local f = node:async_commit(selector.max_ballot, selector.proposal)
		table.insert(commits, f:rawevent())
	end
	-- gathering commit result
	selector.n_tick, selector.n_success = 0, 0
	nevent.select(selector)
	if selector.error or selector.n_success < q then
		logger.warn('paxos', 'commit', 'cannot get quorum', selector.nresp, q)
		-- TODO : sleep?
		goto restart
	end

end
function paxos_index:propose(log)
	return tentacle(self.propose_proc, log)
end
function paxos_index:prepare(ballot)
end
function paxos_index:commit(ballot, proposal)
end
function paxos_index:join(...)
	local narg = select('#', ...)
	if narg == 1 then
		nodes = ...
	elseif narg == 2 then
		nodes = {{...}}
	end
	for _,node in ipairs(nodes) do
		local host = node[1]
		if type(host) == 'string' then
			host = numeric_ipv4_addr_by_host(host)
		end
		local m = uuid.root_uuid_of(host, node[2]):mediator().new(self.id)
		if m then
			logger.notice('mediator for '..tostring(node[1])..'|'..node[2], m)
			table.insert(self.nodes, m)
		end
	end
	self.quorum = math.ceil(#self.nodes / 2)
end
function paxos_index:leave(...)
	local narg = select('#', ...)
	if narg == 1 then
		nodes = ...
	elseif narg == 2 then
		nodes = {{...}}
	end
	for i,del in ipairs(nodes) do
		local host = del[1]
		if type(host) == 'string' then
			host = numeric_ipv4_addr_by_host(host)
		end
		for j,node in ipairs(self.nodes) do
			if uuid.addr(node) == host and uuid.thread_id(node) == del[2] then
				table.remove(self.nodes, j)
			end
		end
	end
	self.quorum = math.ceil(#self.nodes / 2)
end

--[[
	id : this paxos name 
	(multiple paxos consensus sequences are allowed, so used to destinguish each other)
	nodes : string representation of each nodes which involved paxos
]]
local function create(id, quorum, processor)
	return setmetatable({
		id = id,
		last_agrees = {}, 
		last_promisses = {}, 
		quorum = quorum, 
		nodes = {},
		logs = {},
		leader = false,
		closer = event.new()
		processor = processor,
	}, paxos_mt)
end
_M.timeout_sec_count = 5 -- default timeout sec : 5s
function _M.new(id, processor)
	local pxs = paxosmap[id]
	if not pxs then
		pxs = luact.supervise(create, id, processor)
		paxosmap[id] = pxs
	end
	return pxs
end
function _M.find(id)
	return assert(paxosmap[id], "no such paxos group:"..tostring(id))
end
function _M.destroy(id)
	local pxs = paxosmap[id]
	if pxs then
		actor.destroy(pxs)
	end
end

return _M
