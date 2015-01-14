local luact = require 'luact.init'
local pbuf = require 'luact.pbuf'
local serde = require 'luact.serde'
local clock = require 'luact.clock'
local uuid = require 'luact.uuid'

local pulpo = require 'pulpo.init'
local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local event = require 'pulpo.event'

local nodelist = require 'luact.cluster.gossip.nodelist'

local _M = {}

-- cdefs
ffi.cdef [[
typedef enum luact_gossip_proto_type {
	LUACT_GOSSIP_PROTO_CHANGE = 1,
	LUACT_GOSSIP_PROTO_USER = 2,
} luact_gossip_proto_type_t;

typedef struct luact_gossip_proto_sys {
	uint8_t type, state;
	uint16_t thread_id;
	uint32_t machine_id;
	uint32_t version;
	uint16_t protover;
} luact_gossip_proto_sys_t;

typedef struct luact_gossip_proto_nodelist {
	uint32_t size, used;
	luact_gossip_proto_sys_t nodes[0];
} luact_gossip_proto_nodelist_t;

typedef struct luact_gossip_proto_user {
	uint8_t type, padd;
	uint16_t len;
	union {
		char *buf_p; //when send (hack!)
		char buf[1]; //when recv
	};
} luact_gossip_proto_user_t;
]]
local LUACT_GOSSIP_PROTO_CHANGE = ffi.cast('luact_gossip_proto_type_t', "LUACT_GOSSIP_PROTO_CHANGE")
local LUACT_GOSSIP_PROTO_USER = ffi.cast('luact_gossip_proto_type_t', "LUACT_GOSSIP_PROTO_USER")
_M.types = {
	[LUACT_GOSSIP_PROTO_CHANGE] = ffi.typeof('luact_gossip_proto_sys_t'),
	[LUACT_GOSSIP_PROTO_USER] = ffi.typeof('luact_gossip_proto_user_t'),
}


-- proto_join
local proto_sys_index = {}
local proto_sys_mt = {
	__index = proto_sys_index
}
function proto_sys_index:set_node(n)
	self.machine_id = n.machine_id
	self.thread_id = n.thread_id
	self.state = n.state
	self.version = n.version
	self.protover = n.protover
end
function proto_sys_index:length()
	return ffi.sizeof('luact_gossip_proto_sys')
end
function proto_sys_index:copy_to(iovlist)
	iovlist = iovlist:reserve(1)
	iovlist:push(self, self:length())
end
function proto_sys_index:try_invalidate(packet)
	if packet.type ~= self.type then
		return false
	end
	return packet.machine_id == self.machine_id and packet.thread_id == self.thread_id
end
function proto_sys_index:handle(mship)
	mship:handle_node_change(self)
end
ffi.metatype('luact_gossip_proto_sys_t', proto_sys_mt)


-- proto user
local proto_user_index = {}
local proto_user_mt = {
	__index = proto_user_index
}
function proto_user_index:length()
	return ffi.sizeof('luact_gossip_proto_user_t') - ffi.sizeof('char*') + self.len
end
function proto_sys_index:copy_to(iovlist)
	iovlist = iovlist:reserve(2)
	iovlist:push(self, self:length() - self.len)
	iovlist:push(self.buf_p, self.len)
end
function proto_user_index:try_invalidate(packet)
	return false
end
function proto_user_index:handle(mship)
	mship:emit('user', self.buf, self.len)
end
ffi.metatype('luact_gossip_proto_user_t', proto_user_mt)


-- node list
local proto_nodelist_index = {}
local proto_nodelist_mt
proto_nodelist_mt = {
	__index = proto_nodelist_index,
	size = function (sz)
		return ffi.sizeof('luact_gossip_proto_nodelist_t') + sz * ffi.sizeof('luact_gossip_proto_sys_t')
	end,
	alloc = function (size)
		local p = ffi.cast('luact_gossip_proto_nodelist_t*', memory.alloc(proto_nodelist_mt.size(size)))
		p.size = size
		return p
	end,
}
function proto_nodelist_index:reserve(size)
	if size > self.size then
		newsize = self.size
		while newsize < size do
			newsize = newsize * 2
		end
		tmp = ffi.cast('luact_gossip_proto_nodelist_t*', memory.realloc(self, proto_nodelist_mt.size(size)))
		if tmp ~= ffi.NULL then
			tmp.size = newsize
			return tmp
		end
	end
	return self
end
function proto_nodelist_index.pack(arg)
	return ffi.string(arg, proto_nodelist_mt.size(arg.used))
end
function proto_nodelist_index.unpack(arg)
	return ffi.cast('luact_gossip_proto_nodelist_t*', arg)
end
serde[serde.kind.serpent]:customize(
	'struct luact_gossip_proto_nodelist', 
	proto_nodelist_index.pack, proto_nodelist_index.unpack
)
ffi.metatype('luact_gossip_proto_nodelist_t', proto_nodelist_mt)


-- module functions
local sys_cache = {}
local function alloc_sys_packet()
	if #sys_cache > 0 then
		return table.remove(sys_cache)
	else
		return memory.alloc_typed('luact_gossip_proto_sys_t')
	end
end
local user_cache = {}
local function alloc_user_packet()
	if #user_cache > 0 then
		return table.remove(user_cache)
	else
		return memory.alloc_typed('luact_gossip_proto_user_t')
	end
end
function _M.new_change(node)
	local p = alloc_sys_packet()
	p.type = LUACT_GOSSIP_PROTO_CHANGE
	p:set_node(node)
	return p	
end
function _M.new_user(buf, len)
	local p = alloc_user_packet()
	p.type = LUACT_GOSSIP_PROTO_USER
	p.len = len
	p.buf = buf
	return p
end
function _M.destroy(p)
	if ffi.cast('uint8_t *', p)[0] == LUACT_GOSSIP_PROTO_USER then
		table.insert(user_cache, p)
	else
		table.insert(sys_cache, p)
	end
end
function _M.from_ptr(p)
	return ffi.cast(_M[p[0]], p)
end
function _M.new_nodelist(size)
	return proto_nodelist_mt.alloc(size)
end

return _M
