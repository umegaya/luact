--[[
	implementation of cluster membership management and failure detection using gossip based protocol
]]--

local luact = require 'luact.init'
local pbuf = require 'luact.pbuf'
local clock = require 'luact.clock'

local pulpo = require 'pulpo.init'
local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local event = require 'pulpo.event'
local tentacle = require 'pulpo.tentacle'

local nodelist = require 'luact.cluster.gossip.nodelist'
local queue = require 'luact.cluster.gossip.queue'
local protocol = require 'luact.cluster.gossip.protocol'

local _M = {}
local gossip_map = {}


-- cdefs
ffi.cdef [[
typedef struct luact_gossip {
	pulpo_io_t *udp;
	luact_gossip_send_queue_t queue;
	bool enable;
} luact_gossip_t;
]]


-- luact_gossip_t
local gossip_index = {}
local gossip_mt = {
	__index = gossip_index 
}
function gossip_index:init(port, queue_size, mtu)
	self.udp = pulpo.evloop.io.udp.listen('0.0.0.0:'..port)
	self.enable = false
	self.queue:init(queue_size, mtu)
end
function gossip_index:fin(mship)
	self:leave(mship)
	self.udp:close()
	self.queue:fin()
	memory.free(self)
end
function gossip_index:start_reader(mship)
	return tentacle(self.run_reader, self, mship)
end
function gossip_index:run_reader(mship)
	local a = ffi.new('pulpo_addr_t')
	local mtu = mship.opts.mtu
	local buf, len = ffi.new('char[?]', mtu)
	while true do 
		len = self.udp:read(buf, mtu, a)
		self:receive(mship, a, buf, len)
	end
end
function gossip_index:start(mship)
	return tentacle(self.run, self, mship)
end
function gossip_index:run(mship)
	local ev, opts, cooldown = mship.event, mship.opts, 1.0
	while true do
		if self:join(mship) > 0 then
			break
		end
		clock.sleep(cooldown)
		cooldown = cooldown * 2
		if cooldown > opts.startup_timeout then
			ev:emit('error', exception.new('timeout', 'gossip start up', startup_timeout))
			return
		end
	end
	-- start periodic (sub) task
	table.insert(mship.threads, clock.timer(opts.probe_interval, self.probe, self, mship))
	table.insert(mship.threads, clock.timer(opts.exchange_interval, self.exchange, self, mship))
	-- start reader
	table.insert(mship.threads, self:start_reader(mship))
	self.enable = true
	-- notice caller to join gossip cluster
	ev:emit('start', self)
	-- run gossip
	while true do
		clock.sleep(opts.gossip_interval)
		self:gossip(mship)
		mship:check_suspicious_nodes()
	end
end
function gossip_index:broadcast(buf)
	self.queue:push(buf)
end
function gossip_index:receive(mship, addr, payload, plen)
	-- TODO : for WAN gossip, we need to care about endian
	while plen > 0 do
		local data = ffi.cast(protocol[payload[0]], payload)
		payload = payload + data:length()
		plen = plen - data:length()
		data:handle(mship)
	end
end
function gossip_index:gossip(mship)
	local nodes = mship.nodes:k_random(mship.opts.gossip_nodes)
	local vec, len = self.queue:pop(mship.opts.mtu)
	if vec then
		for _, n in ipairs(nodes) do
			self.udp:writev(vec, len, n:address())
		end	
	end
end
function gossip_index:probe(mship)
	local ok, r
	local retry = 0
	local n = mship.nodes:k_random(1)
::RESTART::
	ok, r = pcall(n.actor.timed_ping, n.actor, mship.probe_timeout)
	if not (ok and r) then
		if retry < 3 and r:is('actor_temp_fail') then
			retry = retry + 1
			clock.sleep(retry * 0.5)
			goto RESTART
		end
		-- try indirect ping with using `indirect_checks` nodes
		local ping_nodes = mship.nodes:k_random(mship.opts.indirect_checks)
		local events = {}
		for _, node in ping_nodes do
			table.insert(events, node.actor:indirect_ping(n.actor))
		end
		if 'end' ~= event.wait(function (tp, obj, ok)
			if tp == 'end' and ok then
				return true
			elseif tp == 'read' then
				return true
			end
		end, clock.alarm(mship.opts.probe_timeout), unpack(events)) then
			mship:suspect(n)
		end -- even if success, this node does not regard this node as 'alive' (same as hashicorp/memberlist)
	else
		mship:alive(n)
	end
end
function gossip_index:join(mship)
	local n_join = 0
	for _, node in ipairs(mship.nodes) do
		local ok, r = pcall(self.exchange_with, self, node, mship)
		if ok then
			n_join = n_join + 1
		elseif not (r:is('actor_not_found') or r:is('actor_tmp_fail')) then
			logger.error('exchange data with', node.addr, 'fails', r)
		end
	end
	return n_join
end
function gossip_index:leave(mship, timeout)
	local me = mship.nodes:self()
	logger.info('gossip', 'node', 'leave', me:address())
	if me:set_state(nodelist.dead) then
		self:broadcast(protocol.new_state_change(me))
		local left = timeout or mship.opts.shutdown_timeout
		while left > 0 do
			local start = clock.get()
			-- wait leave is done
			local tp,_,node = event.wait(clock.alarm(left), mship.event)
			if tp == 'leave' and node == me then
				return true
			elseif tp == 'read' then
				return false -- timeout
			end
			left = left - (clock.get() - start)
		end
	end
	return false
end
function gossip_index:exchange(mship)
	local n = mship.nodes:k_random(1)
	self:exchange_with(n, mship)
end
function gossip_index:exchange_with(node, mship)
	local ok, peer_nodes, actor
	local retry = 0
::RESTART::
	actor = node.actor
	ok, peer_nodes = pcall(actor.push_and_pull, actor, mship.nodes:pack())
	if not ok then
		if retry < 3 and peer_nodes:is('actor_temp_fail') then
			retry = retry + 1
			clock.sleep(retry * 0.5)
			goto RESTART
		end
		error(peer_nodes)
	end
	for i=0,tonumber(peer_nodes.used)-1 do
		local nd = peer_nodes.nodes[i]
		mship:add_node(nd)
	end
end
ffi.metatype('luact_gossip_t', gossip_mt)


-- membership_mt
local membership_index = {}
local membership_mt = {
	__index = membership_index
}
function membership_index:__actor_destroy__()
	_M.destroy(self)
end
function membership_index:start()
	-- add self
	self.nodes:add_self()
	-- add initial node
	for _, node in ipairs(self.opts) do
		logger.info('gossip', 'add initial node', node)
		self.nodes:add_by_hostname(node)
	end
	table.insert(self.threads, self.gossip:start(self))
end
function membership_index:push_and_pull(nodes)
	-- nodes : luact_gossip_proto_nodelist_t
	if nodes.used > 0 then
		for i=0,tonumber(nodes.used)-1 do
			local nd = nodes.nodes[i]
			self:add_node(nd)
		end
	end
	return self.nodes:pack()
end
function membership_index:ping()
	return true
end
function membership_index:indirect_ping(redirect_to)
	return redirect_to:ping()
end
function membership_index:broadcast(buf, len)
	self.gossip:broadcast(protocol.new_user_defined(buf, len))
end
function membership_index:leave(timeout)
	self.leave_start = true
	return self.gossip:leave(self, timeout)
end
function membership_index:suspicion_timeout()
	return self.opts.suspicion_factor * math.log10(#self.nodes) * self.opts.probe_interval
end
function membership_index:retransmit()
	return math.floor(self.opts.retransmit_factor * math.log10(#self.nodes))
end
function membership_index:add_node(nodedata, bootstrap)
	local state = nodedata.state
	if state == nodelist.alive then
		self:alive(nodedata, bootstrap)
	elseif state == nodelist.suspect then
		nodedata:suspect(nodedata)
	elseif state == nodelist.dead then
		nodedata:dead(nodedata)
	end
end
function membership_index:handle_node_change(nodedata)
	self:add_node(nodedata)
end
function membership_index:alive(nodedata, bootstrap)
	local n = self.nodes:find_by_nodedata(nodedata)
	local is_my_node = self.nodes:self():has_same_nodedata(nodedata)
	local resurrect, newly_added, changed
	-- It is possible that during a leave(), 
	-- there is already an node_stage_change packet with alive status
	-- in-queue to be processed but blocked by the locks above. If we let it processed, 
	-- it'll cause us to re-join the cluster. This ensures that we don't do that.
	if self.leave_start and is_my_node then
		return
	end
	-- Check if we've never seen this node before, and if not, then
	-- store this node in our node map.
	if not n then
		-- Add to map (and swap with random element)
		n = self.nodes:add_by_nodedata(nodedata, self)
		newly_added = true
	-- ignore if the version number is older, and this is not about us
	elseif nodedata.version <= n.version and (not is_my_node) then
		return
	-- ignore if strictly less and this is about us
	elseif nodedata.version < n.version and is_my_node then
		return
	end

	-- Store the old state and meta data
	if n:is_dead() then
		resurrect = true
	end
	-- oldMeta := state.Meta
	-- If this is us we need to refute, otherwise re-broadcast
	if (not bootstrap) and is_my_node then
		-- If the Incarnation is the same, we need special handling, since it
		-- possible for the following situation to happen:
		-- 1) Start with configuration C, join cluster
		-- 2) Hard fail / Kill / Shutdown
		-- 3) Restart with configuration C', join cluster
		-- 
		-- In this case, other nodes and the local node see the same incarnation,
		-- but the values may not be the same. For this reason, we always
		-- need to do an equality check for this Incarnation. In most cases,
		-- we just ignore, but we may need to refute.
		-- 
		if n.version == nodedata.version then
			return
		end
		n.version = nodedata.version + 1
		self.gossip:broadcast(protocol.new_state_change(n))
		logger.warn('gossip', 'memberlist', 'Refuting an alive message')
		return
	else
		-- Update the state and version number
		n.version = nodedata.version
		changed = n:set_state(nodelist.alive)
	end
	logger.info('gossip', 'node', 'alive', n:address())
	if resurrect or newly_added then
		self:emit('join', n)
	elseif changed then
		self:emit('change', n)
	end
	self.gossip:broadcast(protocol.new_state_change(n))
end
function membership_index:suspect(nodedata)
	local n = self.nodes:find_by_nodedata(nodedata)
	local is_my_node = self.nodes:self():has_same_nodedata(nodedata)
	-- If we've never heard about this node before, ignore it
	if not n then return end
	-- Ignore old incarnation numbers
	if nodedata.version < n.version then return end
	-- Ignore non-alive nodes
	if not n:is_alive() then return end

	-- If this is us we need to refute, otherwise re-broadcast
	if is_my_node then
		n.version = nodedata.version + 1
		self.gossip:broadcast(protocol.new_state_change(n))
		logger.warn('gossip', 'memberlist', ("Refuting a suspect message (from: %s)"):format(n))
		return
	end
	-- Update the state
	n.version = nodedata.version
	if n:set_state(nodelist.suspect) then
		self:emit('change', n)
	end
	self.gossip:broadcast(protocol.new_state_change(n))
	-- add to suspecion check.
	table.insert(self.suspicous_nodes, n)
end
function membership_index:dead(node)
	local n = self.nodes:find_by_nodedata(nodedata)
	local is_my_node = self.nodes:self():has_same_nodedata(nodedata)
	-- If we've never heard about this node before, ignore it
	if not n then return end
	-- Ignore old incarnation numbers
	if nodedata.version < n.version then return end
	-- Ignore non-alive nodes
	if not n:is_alive() then return end

	-- If this is us we need to refute, otherwise re-broadcast
	if is_my_node then
		n.version = nodedata.version + 1
		self.gossip:broadcast(protocol.new_state_change(n))
		logger.warn('gossip', 'memberlist', ("Refuting a dead message (from: %s)"):format(n))
		return
	end
	-- Update the state
	n.version = nodedata.version
	if n:set_state(nodelist.dead) then
		self:emit('leave', n)
	end
	self.gossip:broadcast(protocol.new_state_change(n))
	self.nodes:remove(n)
end
function membership_index:check_suspicious_nodes()
	local now = clock.get()
	for i=1,#self.suspicous_nodes do
		local n = self.suspicous_nodes[i]
		if n:is_suspicious() then
			if (now - n.last_change) > self:suspicion_timeout() then
				self:dead(n)
			end
		else
			table.remove(self.suspicous_nodes, i)
			i = i - 1
		end
	end
end
function membership_index:emit(t, ...)
	if self.delegate then
		self.delegate:memberlist_event(t, ...)
	end
	self.event:emit(t, ...)
end


-- create gossip service mshipen on *port*
local default = {
	startup_timeout = 10,
	shutdown_timeout = 10,
	
	gossip_interval = 0.2,
	gossip_nodes = 3,
	
	probe_interval = 1,
	probe_timeout = 0.5,

	retransmit_factor = 4,	-- Retransmit a message retransmit_factor * log10(# of nodes) times
	suspicion_factor = 5,	-- Suspect a node for suspicion_factor * log10(# of nodes) * probe_interval
	
	exchange_interval = 30,

	indirect_checks = 3,	-- Use 3 nodes for the indirect ping

	mtu = 1024,

	initial_send_queue_size = 4096,
	initial_nodelist_buffer = 256,
}
local function create(port, opts, rv)
	local m = {
		nodes = nodelist.new(port, protocol.new_nodelist(opts.initial_nodelist_buffer)), 
		suspicous_nodes = {}, -- check timeout
		threads = {}, 
		opts = opts,
		delegate = opts.delegate,
		event = event.new(),
	}
	local g = memory.alloc_typed('luact_gossip_t')
	g:init(port, opts.initial_send_queue_size, opts.mtu)
	m.gossip = g
	rv.event = m.event
	return setmetatable(m, membership_mt)
end
_M.create_ev = event.new()
function _M.new(port, opts)
	local rv = {}
	local ent = gossip_map[port]
	local port_num
	if not ent then
		if ent == nil then
			gossip_map[port] = false
			opts = util.merge_table(default, opts or {})
			local a = luact.supervise(create, opts.supervise_options, port, opts, rv)
			ent = {a, rv.event}
			gossip_map[port] = ent
			a:start() -- this refers this gossip object, so after entry to map, call start().
			_M.create_ev:emit('create', port, gossip_map[port])
		else
			while true do 
				port_num, ent = select(3, event.join(clock.alarm(5.0), _M.create_ev))
				if not port then
					exception.raise('gossip', 'object creation timeout')
				end
				if port_num == port then
					break
				end
			end
		end
	end
	return ent[1]
end
function _M.destroy(m)
	for _, t in ipairs(m.threads) do
		tentacle.cancel(t)
	end
	nodelist.destroy(m.nodes)
	m.gossip:fin()
	gossip_map[m.nodes.port] = nil
end

return _M
