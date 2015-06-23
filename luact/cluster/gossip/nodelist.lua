local luact = require 'luact.init'
local pbuf = require 'luact.pbuf'
local serde = require 'luact.serde'
local clock = require 'luact.clock'
local uuid = require 'luact.uuid'
local actor = require 'luact.actor'

local pulpo = require 'pulpo.init'
local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local event = require 'pulpo.event'
local socket = require 'pulpo.socket'

local _M = {}
_M.PROTO_VERSION = 1
_M.MAX_USER_STATE_SIZE = 256


-- cdefs
ffi.cdef [[
typedef struct luact_gossip_node {
	uint32_t clock; //kind of lamport clock
	uint32_t machine_id;
	uint16_t thread_id, protover;
	uint8_t state, user_state_len, padd[2];
	luact_uuid_t actor;
	pulpo_addr_t addr;
	double last_change;
	char user_state[0];
} luact_gossip_node_t;
typedef enum luact_gossip_node_status {
	LUACT_GOSSIP_NODE_ALIVE = 1,
	LUACT_GOSSIP_NODE_SUSPECT = 2,
	LUACT_GOSSIP_NODE_DEAD = 3,
} luact_gossip_node_status_t;
]]
_M.alive = ffi.cast('luact_gossip_node_status_t', "LUACT_GOSSIP_NODE_ALIVE")
_M.suspect = ffi.cast('luact_gossip_node_status_t', "LUACT_GOSSIP_NODE_SUSPECT")
_M.dead = ffi.cast('luact_gossip_node_status_t', "LUACT_GOSSIP_NODE_DEAD")


-- luact_gossip_node_t
local node_index = {}
local cache = {}
local node_mt = {
	__index = node_index,
	__tostring = function (t)
		-- print('tostring', t.addr, t.thread_id)
		return tostring(t.addr).."/"..t.thread_id
	end,
	__gc = function (t)
		logger.report('nodegc', debug.traceback())
	end,
	alloc = function ()
		if #cache > 0 then
			return table.remove(cache)
		else
			return ffi.cast('luact_gossip_node_t*', memory.alloc(ffi.sizeof('luact_gossip_node_t') + _M.MAX_USER_STATE_SIZE))
		end
	end,
	make_key = function (machine_id, thread_id)
		return bit.lshift(machine_id, 16) + thread_id
	end

}
function node_index:init(machine_id, thread_id, port, user_state, user_state_len)
	self.clock = 1
	self.protover = _M.PROTO_VERSION
	self.state = _M.alive
	self.machine_id = machine_id
	self.thread_id = thread_id
	self.addr:set_by_machine_id(self.machine_id, port + thread_id)
	uuid.invalidate(self.actor)
	if user_state_len and (user_state_len > 0) then
		self.user_state_len = user_state_len
		ffi.copy(self.user_state, user_state, user_state_len)
	else
		self.user_state_len = 0
	end
end
function node_index:key()
	return node_mt.make_key(self.machine_id, self.thread_id)
end
function node_index:filter()
	return self:is_alive() and (not self:is_this_node())
end
function node_index:set_state(st, nodedata)
	local changed 
	if nodedata and (nodedata.user_state_len > 0) then
		self.user_state_len = nodedata.user_state_len
		ffi.copy(self.user_state, nodedata.user_state, nodedata.user_state_len)
		changed = true
	end
	if self.state ~= st then
		self.state = st
		self.last_change = clock.get()
		return true
	end
	return changed
end
function node_index:user_state_as(ct)
	assert(self.user_state_len >= ffi.sizeof(ct))
	if type(ct) == "string" then
		ct = ffi.typeof(ct)
	end
	return ffi.cast(ffi.typeof('$ *', ct), self.user_state)
end
function node_index:is_suspicious() 
	return self.state == _M.suspect
end
function node_index:is_dead()
	return self.state == _M.dead
end
function node_index:is_alive()
	return self.state == _M.alive
end
function node_index:is_this_node()
	return self.machine_id == uuid.node_address and self.thread_id == pulpo.thread_id
end
function node_index:address()
	return self.addr
end
function node_index:gossiper()
	if not uuid.valid(self.actor) then
		self.actor = _M.gossiper_from(self.machine_id, self.thread_id, self.addr:port() - self.thread_id)
	end
	return self.actor
end
function node_index:arbiter(group, ...)
	return _M.arbiter_from(self.machine_id, self.thread_id, group, ...)
end
function node_index:has_same_nodedata(nodedata)
	return self.machine_id == nodedata.machine_id and self.thread_id == nodedata.thread_id	
end
function node_index:length(with_user_state) 
	return ffi.sizeof('luact_gossip_node_t') + (with_user_state and self.user_state_len or 0)
end
function node_index:update_user_state(user_state, user_state_len)
	self.user_state_len = user_state_len
	if user_state_len > 0 then
		ffi.copy(self.user_state, user_state, user_state_len)
	end
	self.clock = self.clock + 1 -- update clock so that broadcast accept to other nodes
end
ffi.metatype('luact_gossip_node_t', node_mt)

-- nodelist
local nodelist_index = {}
local nodelist_mt = {
	__index = nodelist_index,
}
function nodelist_index:k_random(k)
	local r = util.random_k_from(self, k, node_index.filter)
	return k == 1 and r[1] or r
end
function nodelist_index:debug_num_valid_nodes()
	local cnt = 0
	for i=1,#self do
		local n = self[i]
		if n:filter() then
			cnt = cnt + 1
		end
	end
	return cnt
end
function nodelist_index:add(n)
	local key = n:key()
	local node = self.lookup[key]
	if node then
		return node
	else
		self.lookup[key] = n
		table.insert(self, n)
		-- Get a random offset and swap with last. This is important to ensure
		-- the failure detection bound is low on average. If all
		-- nodes did an append, failure detection bound would be
		-- very high.
		local offset = util.random(1, #self)
		self[offset], self[#self] = self[#self], self[offset]
		return n
	end
end
function nodelist_index:remove(n)
	local key = n:key()
	local node = self.lookup[key]
	if node then
		-- TODO : faster way to remove entry from list when cluster size is huge.
		self.lookup[key] = nil
		for idx=1,#self do
			if self[idx]:key() == key then
				assert(self[idx] == node)
				table.remove(self, idx)
				break
			end
		end
		table.insert(cache, node)
	end
end
function nodelist_index:create_node(machine_id, thread_id, user_state, user_state_len)
	local n = node_mt.alloc()
	n:init(machine_id, thread_id, self.port, user_state, user_state_len)
	return n
end
function nodelist_index:add_self(user_state, user_state_len)
	local n = self:create_node(uuid.node_address, pulpo.thread_id, user_state, user_state_len)
	self.me = n
	return self:add(n)
end
function nodelist_index:add_by_nodedata(nodedata)
	local n = self:create_node(nodedata.machine_id, nodedata.thread_id, nodedata.user_state, nodedata.user_state_len)
	return self:add(n)
end
function nodelist_index:add_by_hostname(hostname, thread_id)
	local n = self:create_node(socket.numeric_ipv4_addr_by_host(hostname), thread_id)
	return self:add(n)
end
function nodelist_index:add_by_root_actor(root_actor)
	local n = self:create_node(uuid.machine_id(root_actor), uuid.thread_id(root_actor))
	return self:add(n)
end
function nodelist_index:self()
	return self.me
end
function nodelist_index:find_by_nodedata(nodedata)
	return self.lookup[node_mt.make_key(nodedata.machine_id, nodedata.thread_id)]
end	
function nodelist_index:find_by_actor(a)
	return self.lookup[node_mt.make_key(uuid.machine_id(a), uuid.thread_id(a))]
end	
function nodelist_index:pack(with_user_state)
	self.packet_buffer = self.packet_buffer:copy(self, with_user_state)
	return self.packet_buffer
end



-- module function
function _M.new(port, packet_buffer)
	logger.warn('nodelist buffer', packet_buffer)
	return setmetatable({
		lookup = {},
		packet_buffer = packet_buffer,
		port = port,
	}, nodelist_mt)
end
function _M.destroy(l)
	for idx=1,#l do
		local node = l[idx]
		table.insert(cache, node)
	end
end
function _M.gossiper_from(machine_id, thread_id, port)
	return actor.root_of(machine_id, thread_id).gossiper(port)
end
function _M.arbiter_from(machine_id, thread_id, group, ...)
	return actor.root_of(machine_id, thread_id).arbiter(group, ...)
end

return _M