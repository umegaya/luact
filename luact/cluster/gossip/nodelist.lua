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


-- cdefs
ffi.cdef [[
typedef struct luact_gossip_node {
	uint32_t version;
	uint32_t machine_id;
	uint16_t thread_id, protover;
	uint8_t state, padd[3];
	luact_uuid_t actor;
	pulpo_addr_t addr;
	double last_change;
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
local node_tostring_work = memory.alloc_typed('pulpo_addr_t')
local node_mt = {
	__index = node_index,
	__tostring = function (t)
		return tostring(t.addr).."/"..t.thread_id
	end,
	alloc = function ()
		if #cache > 0 then
			table.remove(cache)
		else
			return memory.alloc_typed('luact_gossip_node_t')
		end
	end,
	make_key = function (machine_id, thread_id)
		return bit.lshift(machine_id, 16) + thread_id
	end

}
function node_index:init(machine_id, thread_id, port)
	self.version = 1
	self.protover = _M.PROTO_VERSION
	self.state = _M.alive
	self.machine_id = machine_id
	self.thread_id = thread_id
	self.addr:set_by_machine_id(self.machine_id, port)
	self.actor = _M.gossiper_from(self.machine_id, self.thread_id)
end
function node_index:key()
	return node_mt.make_key(self.machine_id, self.thread_id)
end
function node_index:filter()
	return self:is_alive()
end
function node_index:set_state(st, mship)
	if self.state ~= st then
		self.state = st
		self.last_change = clock.get()
		return true
	end
	return false
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
function node_index:has_same_nodedata(nodedata)
	return self.machine_id == nodedata.machine_id and self.thread_id == nodedata.thread_id	
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
function nodelist_index:add_by_nodedata(nodedata, mship, join)
	return self:add(self:create_node(nodedata.machine_id, nodedata.thread_id), mship, join)
end
function nodelist_index:add(n, mship, join)
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
				table.remove(self, idx)
				break
			end
		end
		table.insert(cache, node)
	end
end
function nodelist_index:create_node(machine_id, thread_id)
	local n = node_mt.alloc()
	n:init(machine_id, thread_id, self.port)
	return n
end
function nodelist_index:add_self()
	local n = self:create_node(uuid.node_address, pulpo.thread_id)
	self.me = n
	return self:add(n)
end
function nodelist_index:add_by_hostname(hostname, thread_id)
	local n = self:create_node(socket.numeric_ipv4_addr_by_host(hostname), thread_id)
	return self:add(n)
end
function nodelist_index:self()
	return self.me
end
function nodelist_index:find_by_nodedata(nodedata)
	return self.lookup[node_mt.make_key(nodedata.machine_id, nodedata.thread_id)]
end	
function nodelist_index:pack()
	-- TODO : faster way to pack this.
	self.packet_buffer = self.packet_buffer:reserve(#self)
	self.used = #self
	for i=1,#self do
		self.packet_buffer.nodes[i - 1]:set_node(self[i])
	end
	return self.packet_buffer
end



-- module function
function _M.new(port, packet_buffer)
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
function _M.gossiper_from(machine_id, thread_id)
	return actor.root_of(machine_id, thread_id).gossiper()
end

return _M