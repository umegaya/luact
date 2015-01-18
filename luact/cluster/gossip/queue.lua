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


-- transfer element
local transfer_element_index = {}
local transfer_element_mt = {
	__index = transfer_element_index
}
function transfer_element_index:dump(tag)
	print(tag, self, self.packet)
end
ffi.metatype('luact_gossip_transter_element_t', transfer_element_mt)


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
function iov_buffer_index:reserve_common(required, type)
	if required <= self.size then
		return self
	end
	local newsize = self.size
	while newsize < required do
		newsize = newsize * 2
	end
	local newp = memory.realloc_typed(type, self, newsize)
	if newp == ffi.NULL then
		exception.raise('fatal', 'fail to malloc', newsize)
	end
	-- print('reserve', self, newp, self.size, newsize, required)
	self.size = newsize
	return self
end
function iov_buffer_index:reserve(size)
	return self:reserve_common(self.used + size, 'luact_gossip_iov_buffer_t')
end
function iov_buffer_index:push(p, l)
	self.list[self.used].iov_base = p
	self.list[self.used].iov_len = l
	-- print(self.used, self.list[self.used].iov_len)
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
function element_buffer_index:fin()
	for i=0, self.used-1 do
		protocol.destroy(self.list[i].packet)
	end
	memory.free(self)
end
function element_buffer_index:reserve(size)
	return self:reserve_common(self.used + size, 'luact_gossip_element_buffer_t')
end
function element_buffer_index:push(retransmit, buf)
	for i=self.used-1,0,-1 do
		if protocol.from_ptr(self.list[i].packet):try_invalidate(buf) then
			self:remove(i)
		end
	end
	self.list[self.used].retransmit = retransmit
	self.list[self.used].packet = ffi.cast('char *', buf)
	self.used = self.used + 1
end
function element_buffer_index:remove(idx)
	protocol.destroy(self.list[idx].packet)
	memory.move(self.list + idx, self.list + idx + 1, 
		(self.used - idx - 1) * ffi.sizeof('luact_gossip_transter_element_t'))
	self.used = self.used - 1
end
local swap_work = memory.alloc_typed('luact_gossip_transter_element_t')
function element_buffer_index:sort()
	-- order by retransmit accending
	if self.used > 1 then
		util.qsort(self.list, 0, self.used - 1, function (a, b) 
			return a.retransmit < b.retransmit
		end, function (x, i, j)
			-- for cdata, x[i], x[j] = x[j], x[i] is not worked as normal lua code
			ffi.copy(swap_work, x + i, ffi.sizeof('luact_gossip_transter_element_t'))
			ffi.copy(x + i, x + j, ffi.sizeof('luact_gossip_transter_element_t'))
			ffi.copy(x + j, swap_work, ffi.sizeof('luact_gossip_transter_element_t'))
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
	self.elembuf = self.elembuf:reserve(1)
	self.elembuf:push(mship:retransmit(), buf)
end
function send_queue_index:used()
	return self.elembuf.used
end
function send_queue_index:pop(mship, mtu)
	local byte_used = 0
	self.iovbuf.used = 0
	for i=self.elembuf.used-1,0,-1 do
		local e = self.elembuf.list[i]
		local pkt = protocol.from_ptr(e.packet)
		if e.retransmit <= 0 then
			pkt:finished(mship)
			self.elembuf:remove(i)
		else
			local len = pkt:length()
			if (byte_used + len) < mtu then
				self.iovbuf = protocol.from_ptr(e.packet):copy_to(self.iovbuf)
				byte_used = byte_used + len
				e.retransmit = e.retransmit - 1
			else
				break
			end
		end
	end
	if self.iovbuf.used > 0 then
		self.elembuf:sort()
		return self.iovbuf.list, self.iovbuf.used
	end
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
	memory.free(p)
end

return _M