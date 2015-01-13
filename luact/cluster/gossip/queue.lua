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

local protocol = require 'luact.cluster.gossip.protocol'

local _M = {}


-- cdefs
ffi.cdef [[
typedef struct luact_gossip_queue_element {
	int retransmit;
	char *packet;
} luact_gossip_transter_element_t;

typedef struct luact_gossip_iov_buffer {
	uint16_t size, used;
	struct iovec list[0];
} luact_gossip_iov_buffer_t;

typedef struct luact_gossip_element_buffer {
	uint16_t size, used;
	luact_gossip_transter_element_t list[0];
} luact_gossip_element_buffer_t;

typedef struct luact_gossip_send_queue {
	luact_gossip_element_buffer_t *elembuf;
	luact_gossip_iov_buffer_t *iovbuf;
} luact_gossip_send_queue_t;
]]


-- iov buffer
local iov_buffer_index = {}
local iov_buffer_mt = {
	__index = iov_buffer_index,
	alloc = function (size)
		local p = ffi.cast('luact_gossip_iov_buffer_t*', 
			memory.alloc(
				ffi.sizeof('luact_gossip_iov_buffer_t') + 
				ffi.sizeof('struct iovec') * size
			)
		)
		p:init(size)
		return p
	end
}
function iov_buffer_index:init(size)
	self.size = size
	self.used = 0
end
function iov_buffer_index:fin()
	memory.free(self)
end
function iov_buffer_index:reserve(size)
	local required = (self.used + size)
	if required <= self.size then
		return self
	end
	local newsize = self.size
	while newsize >= required do
		newsize = newsize * 2
	end
	local newp = memory.realloc(self, newsize)
	if newp == ffi.NULL then
		exception.raise('fatal', 'fail to malloc', newsize)
	end
	self = ffi.cast('luact_gossip_iov_buffer_t*', newp)
	self.size = newsize
	return self
end
function iov_buffer_index:push(p, l)
	self.list[self.used].iov_base = p
	self.list[self.used].iov_len = l
	self.used = self.used + 1
end
ffi.metatype('luact_gossip_iov_buffer_t', iov_buffer_mt)


-- element buffer
local element_buffer_index = util.copy_table(iov_buffer_index)
local element_buffer_mt = {
	__lt = element_buffer_lt,
	__index = element_buffer_index,
	alloc = function (size)
		local p = ffi.cast('luact_gossip_element_buffer_t*', 
			memory.alloc(
				ffi.sizeof('luact_gossip_element_buffer_t') + 
				ffi.sizeof('luact_gossip_transter_element_t') * size
			)
		)
		p:init(size)
		return p
	end,
}
function iov_buffer_index:fin()
	for i=0, self.used-1 do
		protocol.destroy(self.list[i].packet)
	end
	memory.free(self)
end
function element_buffer_index:push(retransmit, buf)
	for i=self.used-1,0,-1 do
		if self.list[i]:try_invalidate(buf) then
			self:remove(i)
		end
	end
	self.list[self.used].retransmit = retransmit
	self.list[self.used].packet = buf
	self.used = self.used + 1
end
function element_buffer_index:remove(idx)
	local removed = self.list[idx]
	memory.move(self.list + idx, self.list + idx + 1, self.used - idx - 1)
	protocol.destroy(removed.packet)
end
function element_buffer_index:sort()
	-- order by retransmit accending
	if self.used > 1 then
		util.qsort(self.list, 0, self.used - 1, function (a, b) 
			return a.retransmit < b.retransmit
		end)
	end
end
ffi.metatype('luact_gossip_element_buffer_t', element_buffer_mt)


-- send queue
local send_queue_index = {}
local send_queue_mt = {
	__index = send_queue_index
}
function send_queue_index:init(size, mtu)
	self.iovbuf = iov_buffer_mt.alloc(mtu / 16) -- minimum packet size is around 16 byte
	self.elembuf = element_buffer_mt.alloc(size)
end
function send_queue_index:fin()
	self.iovbuf:fin()
	self.elembuf:fin()
end
function send_queue_index:push(mship, buf)
	self.elembuf:reserve(1)
	self.elembuf:push(mship:retransmit(), buf)
end
function send_queue_index:pop(mtu)
	local byte_used = 0
	self.iovbuf.used = 0
	for i=self.elembuf.used-1,0,-1 do
		local e = self.elembuf.list[i]
		local len = e:length()
		if (byte_used + len) < mtu then
			protocol.from_ptr(e.p):copy_to(self.iovbuf)
			byte_used = byte_used + len
			e.retransmit = e.retransmit - 1
			if e.retransmit <= 0 then
				self.elembuf:remove(i)
			end
		else
			break
		end
	end
	if self.iovbuf.used > 0 then
		self.elembuf:sort()
	end
	return self.iovbuf.list, self.iovbuf.used
end
ffi.metatype('luact_gossip_send_queue_t', send_queue_mt)


-- module functions
function _M.new(size, mtu)
	local p = memory.alloc_typed('luact_gossip_send_queue_t')
	p:init(size, mtu)
	return p
end
function _M.destroy(p)
	p:fin()
end

return _M