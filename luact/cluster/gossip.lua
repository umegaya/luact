--[[
	implementation of cluster membership management and failure detection using gossip based protocol
]]--

local luact = require 'luact.init'
local pbuf = require 'luact.pbuf'
local clock = require 'luact.clock'
local actor = require 'luact.actor'

local pulpo = require 'pulpo.init'
local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local event = require 'pulpo.event'
local tentacle = require 'pulpo.tentacle'
local lamport = require 'pulpo.lamport'
-- tentacle.TRACE = true

local nodelist = require 'luact.cluster.gossip.nodelist'
local queue = require 'luact.cluster.gossip.queue'
local protocol = require 'luact.cluster.gossip.protocol'
local ringbuf = require 'luact.util.ringbuf'

local _M = {}
local gossip_map = {}
local gossip_event_map = {}

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
	self.udp = pulpo.evloop.io.udp.listen('0.0.0.0:'..(port + pulpo.thread_id))
	self.enable = false
	self.queue:init(queue_size, mtu)
end
function gossip_index:fin(mship)
	self:leave(mship)
	self.udp:close()
	self.queue:fin()
	memory.free(self)
end
function gossip_index:start(mship)
	return tentacle(self.run, self, mship)
end
function gossip_index:start_reader(mship)
	return tentacle(self.run_reader, self, mship)
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
	if mship.opts.local_mode then
		local a = actor.root_of(nil, 1).gossiper(mship.nodes.port)
		table.insert(mship.threads, clock.timer(opts.exchange_interval, self.exchange_with, self, a, mship))
	else
		-- start periodic (sub) task
		table.insert(mship.threads, clock.timer(opts.probe_interval, self.probe, self, mship))
		table.insert(mship.threads, clock.timer(opts.exchange_interval, self.exchange, self, mship))
	end
	-- start reader
	table.insert(mship.threads, self:start_reader(mship))
	self.enable = true
	-- notice caller to join gossip cluster
	ev:emit('start')
	-- run gossip
	while true do
		clock.sleep(opts.gossip_interval)
		self:gossip(mship)
		mship:check_suspicious_nodes()
	end
end
function gossip_index:broadcast(mship, buf)
	self.queue:push(mship, buf)
end
function gossip_index:receive(mship, payload, plen)
	-- TODO : for WAN gossip, we need to care about endian
	while plen > 0 do
		if not payload then
			logger.warn('payload invalid', payload)
		end
		local data = protocol.from_ptr(payload)
		payload = payload + data:length()
		plen = plen - data:length()
		data:handle(mship)
	end
end
function gossip_index:run_reader(mship)
	local a = ffi.new('pulpo_addr_t')
	local mtu = mship.opts.mtu
	local buf, len = ffi.new('char[?]', mtu)
	while true do 
		len = self.udp:read(buf, mtu, a)
		if len then
			-- logger.info('receive', len)
			self:receive(mship, buf, len)
		end
	end
end
function gossip_index:gossip(mship)
	local nodes = mship.nodes:k_random(mship.opts.gossip_nodes)
	for _, n in ipairs(nodes) do
		-- it may resume gossip_index:leave
		local vec, len = self.queue:pop(mship, mship.opts.mtu)
		if len then
			-- logger.info('sendgossip', n:address())
			self.udp:writev(vec, len, n:address())
		end
	end
end
function gossip_index:probe(mship)
	local ok, r
	local retry = 0
	local n = mship.nodes:k_random(1)
::RESTART::
	ok, r = pcall(n:gossiper().timed_ping, n:gossiper(), mship.opts.probe_timeout)
	if not (ok and r) then
		if retry < 3 and r:is('actor_temporary_fail') then
			retry = retry + 1
			clock.sleep(retry * 0.5)
			goto RESTART
		end
		-- try indirect ping with using `indirect_checks` nodes
		local ping_nodes = mship.nodes:k_random(mship.opts.indirect_checks)
		local events = {}
		for _, node in ipairs(ping_nodes) do
			table.insert(events, node:gossiper():async_timed_indirect_ping(mship.opts.probe_timeout, n:gossiper()))
		end
		local r = event.wait(function (data)
			local tp, _, ok = unpack(data)
			if tp == 'end' and ok then
				return true
			elseif tp == 'read' then
				return true
			end
		end, clock.alarm(mship.opts.probe_timeout), unpack(events))
		if 'end' ~= r then
			mship:mark_suspect(n)
		end -- even if success, this node does not regard this node as 'alive' (same as hashicorp/memberlist)
	else
		mship:alive(n)
	end
end
function gossip_index:join(mship)
	local n_join = 0
	for _, node in ipairs(mship.nodes) do
		if not node:is_this_node() then
			local ok, r = pcall(self.exchange_with, self, node:gossiper(), mship, true)
			if ok then
				n_join = n_join + 1
			elseif not r.is then
				logger.error('invalid response', tostring(r))
			elseif not (r:is('actor_not_found') or r:is('actor_temporary_fail')) then
				logger.error('exchange data with', node.addr, 'fails', r)
			end
		end
	end
	return n_join
end
function gossip_index:leave(mship, timeout)
	if not self.enable then
		return 
	end
	local me = mship.nodes:self()
	logger.info('gossip', 'node', 'leave', me)
	if me:set_state(nodelist.dead) then
		self:broadcast(mship, protocol.new_change(me))
		local left = timeout or mship.opts.shutdown_timeout
		while left > 0 do
			clock.sleep(0.5)
			-- broadcast leave is done
			if mship:leave_sent() then
				return true
			end
			left = left - 0.5
			if left < 0 then
				break
			end
		end
	end
	return false
end
function gossip_index:exchange(mship)
	local n = mship.nodes:k_random(1)
	self:exchange_with(n:gossiper(), mship)
end
function gossip_index:exchange_with(target_actor, mship, join)
	local ok, peer_nodes
	local retry = 0
::RESTART::
	ok, peer_nodes = pcall(target_actor.push_and_pull, target_actor, mship.nodes:pack(join), join)
	if not ok then
		if retry < 3 and peer_nodes:is('actor_temporary_fail') then
			retry = retry + 1
			clock.sleep(retry * 0.5)
			goto RESTART
		end
		error(peer_nodes)
	end
	logger.info('gossip', 'exchange_with', target_actor, peer_nodes.size)
	local actor_node = mship.nodes:find_by_actor(target_actor)
	for _,nd in peer_nodes:iter() do
		mship:add_node(nd)
		if join then
			if actor_node:has_same_nodedata(nd) then
				actor_node:set_state(nodelist.alive, nd)
			end
		end
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
function membership_index:user_state()
	if self.delegate then
		return self.delegate:user_state()
	end
end
function membership_index:start()
	-- add self
	self.nodes:add_self(self:user_state())
	assert(#self.nodes > 0, "invalid node data")
	-- add initial node
	for _, node in ipairs(self.opts) do
		if type(node) == 'string' then
			logger.notice('gossip', 'add initial node', 'hostname', node, #self.nodes)
			self.nodes:add_by_hostname(node)
		elseif type(node) == 'cdata' then
			logger.notice('gossip', 'add initial node', 'actor', node, #self.nodes)
			self.nodes:add_by_root_actor(node)
		else
			exception.raise('invalid', 'node config', type(node))
		end
	end
	table.insert(self.threads, self.gossip:start(self))
end
function membership_index:wait_bootstrap(timeout)
	if not self.gossip.enable then
		while true do 
			local t = event.wait(nil, clock.alarm(timeout), self.event)
			-- logger.info('wait_bootstrap', t)
			if t == 'start' then
				return true
			elseif t == 'read' then
				return false
			end
		end
	end
	return true
end
function membership_index:push_and_pull(nodes, join)
	-- nodes : luact_gossip_proto_nodelist_t
	if nodes.used > 0 then
		for _,nd in nodes:iter() do
			self:add_node(nd)
		end
	end
	return self.nodes:pack(join)
end
function membership_index:pull()
	return self.nodes:pack()
end
function membership_index:ping()
	return true
end
function membership_index:restart()
	exception.raise('actor_error', 'simulate_error')
end
function membership_index:indirect_ping(redirect_to)
	return redirect_to:ping()
end
function membership_index:broadcast(buf, clock)
	self.gossip:broadcast(self, protocol.new_user(memory.strdup(buf), #buf, 
		clock or self.msg_checker:issue_clock()))
end
function membership_index:sys_broadcast(packet)
	self.gossip:broadcast(self, packet)
end
function membership_index:broadcast_user_state()
	if self.delegate then
		local me = self.nodes:self()
		me:update_user_state(self.delegate:user_state())
		self:sys_broadcast(protocol.new_change(me, true))
	end
end
function membership_index:leave(timeout)
	self.leave_start = true
	if self.gossip:leave(self, timeout) then
		self:stop_threads()
		self.enable = false
		return true
	end
	return false
end
function membership_index:leave_sent()
	return self.leave_sent_finished
end
function membership_index:set_leave_sent()
	self.leave_sent_finished = true
end
function membership_index:suspicion_timeout()
	return self.opts.suspicion_factor * math.log10(#self.nodes) * self.opts.probe_interval
end
function membership_index:retransmit()
	return math.floor(self.opts.retransmit_factor * math.log10(#self.nodes))
end
function membership_index:stop_threads()
	for idx, t in ipairs(self.threads) do
		-- logger.info('stop_threads', t)
		tentacle.cancel(t)
		self.threads[idx] = nil
	end
end
function membership_index:add_node(nodedata, bootstrap)
	local state = nodedata.state
	-- logger.info('add_node:state:', state)
	if state == nodelist.alive then
		self:alive(nodedata, bootstrap)
	elseif state == nodelist.suspect then
		self:suspect(nodedata)
	elseif state == nodelist.dead then
		self:suspect(nodedata)
	else
		exception.raise('invalid', 'gossip node state', state)
	end
end
function membership_index:handle_node_change(nodedata)
	self:add_node(nodedata)
end
function membership_index:handle_user_message(message)
	local msg = ffi.string(message.buf, message.len)
	if self.msg_checker:fresh(message.clock, msg) then
		self:emit('user', message.buf, message.len)
		self:broadcast(msg, message.clock)
	end
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
		n = self.nodes:add_by_nodedata(nodedata)
		newly_added = true
	-- ignore if the clock number is older, and this is not about us
	elseif nodedata.clock <= n.clock and (not is_my_node) then
		return
	-- ignore if strictly less and this is about us
	elseif nodedata.clock < n.clock and is_my_node then
		return
	end

	-- Store the old state and meta data
	if n:is_dead() then
		resurrect = true
	end
	-- oldMeta := state.Meta
	-- If this is us we need to refute, otherwise re-broadcast
	if (not bootstrap) and is_my_node then
		if newly_added then
			assert(false, "myself is never treated as newly added node")
		end
		-- If the clock is the same, we need special handling, since it
		-- possible for the following situation to happen:
		-- 1) Start with configuration C, join cluster
		-- 2) Hard fail / Kill / Shutdown
		-- 3) Restart with configuration C', join cluster
		-- 
		-- In this case, other nodes and the local node see the same clock,
		-- but the values may not be the same. For this reason, we always
		-- need to do an equality check for this Incarnation. In most cases,
		-- we just ignore, but we may need to refute.
		-- 
		if n.clock == nodedata.clock then
			return
		end
		n.clock = nodedata.clock + 1
		self:sys_broadcast(protocol.new_change(n))
		logger.warn('gossip', 'node', 'Refuting an alive message')
		return
	else
		-- Update the state and clock number
		n.clock = nodedata.clock
		changed = n:set_state(nodelist.alive, nodedata)
	end
	logger.info('gossip', 'node', 'alive', n)
	if resurrect or newly_added then
		self:emit('join', n, #self.nodes)
	elseif changed then
		self:emit('change', n)
	end
	self:sys_broadcast(protocol.new_change(n, true))
end
function membership_index:suspect(nodedata)
	local n = self.nodes:find_by_nodedata(nodedata)
	local is_my_node = self.nodes:self():has_same_nodedata(nodedata)
	-- If we've never heard about this node before, ignore it
	if not n then return end
	logger.info('gossip', 'node', 'suspect', n)
	-- Ignore old clock numbers
	if nodedata.clock < n.clock then return end
	-- Ignore non-alive nodes
	if not n:is_alive() then return end

	-- If this is us we need to refute, otherwise re-broadcast
	if is_my_node then
		n.clock = nodedata.clock + 1
		self:sys_broadcast(protocol.new_change(n))
		logger.warn('gossip', 'memberlist', ("Refuting a suspect message (from: %s)"):format(n))
		return
	end
	-- Update the state
	n.clock = nodedata.clock
	self:mark_suspect(n, nodedata)
end
function membership_index:mark_suspect(n, nodedata) 
	if n:set_state(nodelist.suspect, nodedata) then
		self:emit('change', n)
	end
	self:sys_broadcast(protocol.new_change(n))
	-- add to suspecion check.
	table.insert(self.suspicous_nodes, n)
end
function membership_index:dead(nodedata)
	local n = self.nodes:find_by_nodedata(nodedata)
	local is_my_node = self.nodes:self():has_same_nodedata(nodedata)
	-- If we've never heard about this node before, ignore it
	if not n then return end
	logger.info('gossip', 'node', 'dead', n, n:is_alive(), nodedata.clock, n.clock)
	-- Ignore old clock numbers
	if nodedata.clock < n.clock then return end
	-- Ignore non-alive nodes
	if n:is_dead() then return end
	-- If this is us we need to refute, otherwise re-broadcast
	if is_my_node then
		n.clock = nodedata.clock + 1
		self:sys_broadcast(protocol.new_change(n))
		logger.warn('gossip', 'node', ("Refuting a dead message (from: %s)"):format(n))
		return
	end
	-- Update the state
	n.clock = nodedata.clock
	if n:set_state(nodelist.dead, nodedata) then
		self:emit('leave', n)
	end
	self:sys_broadcast(protocol.new_change(n))
	self.nodes:remove(n)
end
function membership_index:check_suspicious_nodes()
	local now = clock.get()
	for i=#self.suspicous_nodes,1,-1 do
		local n = self.suspicous_nodes[i]
		if n:is_suspicious() then
			if (now - n.last_change) > self:suspicion_timeout() then
				self:dead(n)
			end
		else
			table.remove(self.suspicous_nodes, i)
		end
	end
end
function membership_index:emit(t, ...)
	logger.notice('gossip:emit', t, ...)
	if self.delegate then
		self.delegate:memberlist_event(t, ...)
	end
	self.event:emit(t, ...)
end
function membership_index:probe(prober, ...)
	return pcall(prober, self, ...)
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
	initial_nodelist_size = 256,

	user_message_bucket_size = 512, 

	local_mode = false, 	-- only communicate with node which is on same host and thread_id 1
}
local function create(port, opts)
	local d = opts.delegate
	local m = setmetatable({
		nodes = nodelist.new(port, protocol.new_nodelist(opts.initial_nodelist_size)), 
		suspicous_nodes = {}, -- check timeout
		threads = {}, 
		opts = opts,
		msg_checker = lamport.new(opts.user_message_bucket_size),
		delegate = (type(d) == 'function' and d(unpack(opts.delegate_args or {})) or d),
		event = event.new(),
	}, membership_mt)
	local g = memory.alloc_typed('luact_gossip_t')
	g:init(port, opts.initial_send_queue_size, opts.mtu)
	m.gossip = g
	gossip_event_map[port] = m.event
	m:start()
	return m
end
_M.create_ev = event.new()
function _M.new(port, opts)
	local g = gossip_map[port]
	local port_num
	if not g then
		if g == nil then
			gossip_map[port] = false
			opts = util.merge_table(default, opts or {})
			g = luact.supervise(create, opts.supervise_options, port, opts)
			gossip_map[port] = g
			_M.create_ev:emit('create', port, g)
		else
			while true do 
				port_num, g = select(3, event.wait(nil, clock.alarm(5.0), _M.create_ev))
				if not port_num then
					exception.raise('actor_timeout', 'gossip', 'object creation timeout', port)
				end
				if port_num == port then
					break
				end
			end
		end
	end
	return g
end
function _M.event(port)
	return gossip_event_map[port]
end
function _M.destroy(m)
	m:stop_threads()
	nodelist.destroy(m.nodes)
	lamport.destroy(m.msg_checker)
	m.gossip:fin(m)
	logger.report('gossip destroy', m.nodes.port)
	gossip_event_map[m.nodes.port] = nil
end

return _M
