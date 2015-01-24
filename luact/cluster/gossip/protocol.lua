local luact = require 'luact.init'
local pbuf = require 'luact.pbuf'
local serde = require 'luact.serde'
local common = require 'luact.serde.common'
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
	uint32_t clock;
	uint16_t protover;
	uint8_t user_state_len, padd;
	char user_state[0]; //when recv
} luact_gossip_proto_sys_t;

typedef struct luact_gossip_proto_nodelist {
	uint32_t size, used;
	char buffer[0];
} luact_gossip_proto_nodelist_t;

typedef struct luact_gossip_proto_user {
	uint8_t type, padd;
	uint16_t len;
	pulpo_lamport_clock_t clock;
	union {
		char *buf_p; //when send
		char buf[1]; //when recv
	};
} luact_gossip_proto_user_t;
]]
local LUACT_GOSSIP_PROTO_CHANGE = ffi.cast('luact_gossip_proto_type_t', "LUACT_GOSSIP_PROTO_CHANGE")
local LUACT_GOSSIP_PROTO_USER = ffi.cast('luact_gossip_proto_type_t', "LUACT_GOSSIP_PROTO_USER")
_M.types = {
	[tonumber(LUACT_GOSSIP_PROTO_CHANGE)] = ffi.typeof('luact_gossip_proto_sys_t*'),
	[tonumber(LUACT_GOSSIP_PROTO_USER)] = ffi.typeof('luact_gossip_proto_user_t*'),
}


-- proto_join
local proto_sys_index = {}
local proto_sys_mt = {
	__index = proto_sys_index
}
function proto_sys_index:set_node(n, with_user_state)
	self.machine_id = n.machine_id
	self.thread_id = n.thread_id
	self.state = n.state
	self.clock = n.clock
	self.protover = n.protover
	if with_user_state and (n.user_state_len > 0) then
		-- logger.report('user_statelen:', n, n.user_state_len, ffi.cast('int *', n.user_state)[0])
		self.user_state_len = n.user_state_len
		ffi.copy(self.user_state, n.user_state, n.user_state_len)
	else
		self.user_state_len = 0
	end
end
function proto_sys_index:length()
	return ffi.sizeof('luact_gossip_proto_sys_t') + self.user_state_len
end
function proto_sys_index:copy_to(iovlist)
	iovlist = iovlist:reserve(1)
	iovlist:push(self, ffi.sizeof('luact_gossip_proto_sys_t'))
	if self.user_state_len > 0 then
		-- logger.report('user_statelen2:', self.user_state_len, ffi.cast('int *', self.user_state)[0])
		iovlist:push(self.user_state, self.user_state_len)
	end
	return iovlist
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
function proto_sys_index:is_this_node()
	return self.machine_id == uuid.node_address and self.thread_id == pulpo.thread_id
end
function proto_sys_index:finished(mship)
	if self:is_this_node() and (self.state == nodelist.dead) then
		mship:set_leave_sent()
	end
end
function proto_sys_index:dump_user_state(tag)
	logger.info('user_state', tostring(tag), self.user_state_len, ffi.cast('int *', self.user_state)[0], self.thread_id)
end
ffi.metatype('luact_gossip_proto_sys_t', proto_sys_mt)


-- proto user
local proto_user_index = {}
local proto_user_mt = {
	__index = proto_user_index
}
function proto_user_index:length()
	-- print(self, ffi.string(self.buf_p))
	return ffi.sizeof('luact_gossip_proto_user_t') - ffi.sizeof('char*') + self.len
end
function proto_user_index:copy_to(iovlist)
	iovlist = iovlist:reserve(2)
	iovlist:push(self, ffi.sizeof('luact_gossip_proto_user_t') - ffi.sizeof('char*'))
	iovlist:push(self.buf_p, self.len)
	return iovlist
end
function proto_user_index:try_invalidate(packet)
	return false
end
function proto_user_index:handle(mship)
	mship:handle_user_message(self)
end
function proto_user_index:finished(mship)
end
ffi.metatype('luact_gossip_proto_user_t', proto_user_mt)


-- node list
local proto_nodelist_index = {}
local proto_nodelist_mt
proto_nodelist_mt = {
	__index = proto_nodelist_index,
	size = function (sz)
		return ffi.sizeof('luact_gossip_proto_nodelist_t') + sz
	end,
	alloc = function (size, managed)
		local p = managed and 
			ffi.cast('luact_gossip_proto_nodelist_t*', memory.alloc(proto_nodelist_mt.size(size))) or
			ffi.cast('luact_gossip_proto_nodelist_t*', memory.managed_alloc(proto_nodelist_mt.size(size)))
		p.size = size
		return p
	end,
}
function proto_nodelist_index:reserve(size)
	local required = (self.used + size)
	if required > self.size then
		newsize = self.size
		while newsize < required do
			newsize = newsize * 2
		end
		tmp = ffi.cast('luact_gossip_proto_nodelist_t*', memory.realloc(self, proto_nodelist_mt.size(newsize)))
		if tmp ~= ffi.NULL then
			tmp.size = newsize
			logger.warn('reserve:', self, "~~>", tmp)
			return tmp
		end
	end
	return self
end
function proto_nodelist_index:fin()
	memory.free(self)
end
function proto_nodelist_index:iter() 
	return function (l, ofs)
		if ofs >= l.used then
			return nil 
		else
			local ent = ffi.cast('luact_gossip_proto_sys_t *', l.buffer + ofs)
			ofs = ofs + ent:length()
			return ofs, ent
		end
	end, self, 0
end
function proto_nodelist_index:copy(list, with_user_state)
	self.used = 0
	for i=1,#list do
		local l = list[i]:length(with_user_state)
		self = self:reserve(l)
		local ofs = ffi.sizeof('luact_gossip_proto_nodelist_t') + self.used
		local buf = ffi.cast('luact_gossip_proto_sys_t*', ffi.cast('char *', self) + ofs)
		buf:set_node(list[i], with_user_state)
		-- buf:dump_user_state('copy')
		self.used = self.used + buf:length()
	end
	return self
end
function proto_nodelist_index:dump(tag)
	for _, ent in self:iter() do
		ent:dump_user_state(tag)
	end
end
local nodelist_unpack_buffer
function proto_nodelist_index.pack(arg)
	return ffi.string(arg.buffer, arg.used)
end
function proto_nodelist_index.unpack(arg)
	nodelist_unpack_buffer = nodelist_unpack_buffer:reserve(#arg)
	ffi.copy(nodelist_unpack_buffer.buffer, arg, #arg)
	nodelist_unpack_buffer.used = #arg
	-- nodelist_unpack_buffer:dump('unpack')
	return nodelist_unpack_buffer
end
ffi.metatype('luact_gossip_proto_nodelist_t', proto_nodelist_mt)
-- register ctype and custom serde
serde[serde.kind.serpent]:customize(
	'struct luact_gossip_proto_nodelist', 
	proto_nodelist_index.pack, proto_nodelist_index.unpack
)
common.register_ctype('struct', 'luact_gossip_proto_nodelist', {
	msgpack = {
		packer = function (pack_procs, buf, ctype_id, obj, length)
			buf:reserve(obj.used)
			local p, ofs = pack_procs.pack_ext_cdata_header(buf, obj.used, ctype_id)
			ffi.copy(p + ofs, obj.buffer, obj.used)
			return ofs + obj.used
		end,
		unpacker = function (rb, len)
			local ptr = proto_nodelist_mt.alloc(len)
			ptr.used = len
			ffi.copy(ptr.buffer, rb:curr_byte_p(), len)
			rb:seek_from_curr(len)
			return ptr
		end,
	}, 
}, common.LUACT_GOSSIP_NODELIST)


-- module functions
local sys_cache = {}
local function alloc_sys_packet()
	if #sys_cache > 0 then
		return table.remove(sys_cache)
	else
		return ffi.cast('luact_gossip_proto_sys_t*', memory.alloc(
			ffi.sizeof('luact_gossip_proto_sys_t') + nodelist.MAX_USER_STATE_SIZE
		))
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
function _M.new_change(node, with_user_state)
	local p = alloc_sys_packet()
	p.type = LUACT_GOSSIP_PROTO_CHANGE
	p:set_node(node, with_user_state)
	return p	
end
function _M.new_user(buf, len, clock)
	local p = alloc_user_packet()
	p.type = LUACT_GOSSIP_PROTO_USER
	p.len = len
	p.buf_p = buf
	p.clock = clock
	return p
end
function _M.destroy(p)
	p = _M.from_ptr(p)
	if p.type == LUACT_GOSSIP_PROTO_USER then
		memory.free(p.buf_p)
		table.insert(user_cache, ffi.cast('luact_gossip_proto_user_t*', p))
	else
		table.insert(sys_cache, ffi.cast('luact_gossip_proto_sys_t*', p))
	end
end
function _M.from_ptr(p)
	if ffi.cast('uint8_t *', p)[0] > 2 then
		logger.error('type id', ffi.cast('uint8_t *', p)[0])
	end
	return ffi.cast(_M.types[ffi.cast('uint8_t *', p)[0]], p)
end
function _M.new_nodelist(size)
	return proto_nodelist_mt.alloc(size)
end
nodelist_unpack_buffer = _M.new_nodelist(4096)

return _M
