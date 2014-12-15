local ffi = require 'ffiex.init'

local clock = require 'luact.clock'

local pulpo = require 'pulpo.init'
local memory = require 'pulpo.memory'
local socket = require 'pulpo.socket'
local exception = require 'pulpo.exception'
local _M = {}
local C = ffi.C

-- constant
_M.THREAD_BIT_SIZE = 12
_M.TIMESTAMP_BIT_SIZE = 42
_M.SERIAL_BIT_SIZE = 64 - (_M.THREAD_BIT_SIZE + _M.TIMESTAMP_BIT_SIZE)
_M.MAX_SERIAL_ID = (bit.lshift(1, _M.SERIAL_BIT_SIZE) - 1)

-- cdefs
ffi.cdef(([[
typedef union luact_uuid {
	struct luact_id_detail {	//luact implementation detail
		uint32_t thread_id:%d; 	//upto 4k core
		uint32_t serial:%d; 	//can generate 1024 actor id/msec/thread
		uint32_t timestamp_hi:%d;	//hi 10 bits of 42bit msec timestamp 
		uint32_t timestamp_lo:32;	//low 32 bits of 42bit msec timestamp 
		uint32_t machine_id:32;	//cluster local ip address
	} __detail__;
	struct luact_id_tag {
		uint64_t local_id;
		uint32_t machine_id;
	} __attribute__((__packed__)) __tag__; //force packed to 12byte
	struct luact_id_tag2 {
		uint32_t local_id[2];
		uint32_t machine_id;
	} __tag2__;
} luact_uuid_t;
]]):format(_M.THREAD_BIT_SIZE, _M.SERIAL_BIT_SIZE, _M.TIMESTAMP_BIT_SIZE - 32))

assert(ffi.sizeof('struct luact_id_detail') == 12)
assert(ffi.sizeof('struct luact_id_tag2') == 12)
assert(ffi.sizeof('struct luact_id_tag') == 12)
assert(ffi.sizeof('union luact_uuid') == 12)

-- vars
local idgen = {
	seed = ffi.new('luact_uuid_t'), 
	availables = {}, 
	new = function (self)
		if #self.availables > 0 then
			buf = table.remove(self.availables)
		else
			buf = ffi.new('luact_uuid_t') -- because wanna use gc
			buf.__detail__.machine_id = self.seed.__detail__.machine_id
			buf.__detail__.thread_id = self.seed.__detail__.thread_id
		end
		return buf
	end,
	free = function (self, uuid)
		table.insert(self.availables, uuid)
	end,
}
local epoc

-- local functions
local function msec_timestamp()
	local s,us = pulpo.util.clock_pair()
	return ((s + us / 1000) * 1000) - epoc
end

-- module function 
function _M.timestamp(t) 
	return (epoc + bit.lshift(t.__detail__.timestamp_hi, 32) + t.__detail__.timestamp_lo) 
end
function _M.set_timestamp(t, tv)
	t.__detail__.timestamp_hi = tv / (2 ^ 32)
	t.__detail__.timestamp_lo = tv % 0xFFFFFFFF
end
function _M.thread_id(t) 
	return t.__detail__.thread_id 
end
function _M.local_id(t) 
	return t.__tag__.local_id 
end
function _M.serial(t) -- local_id without thread_id
	return bit.bor(bit.lshift(_M.timestamp(t), _M.SERIAL_BIT_SIZE), t.__detail__.serial)
end
function _M.addr(t) 
	return t.__tag__.machine_id 
end
_M.machine_id = _M.addr
function _M.clone(t)
	local buf = idgen:new()
	buf.local_id = t.local_id
	return buf
end
function _M.equals(t, cmp)
	return t.__tag__.local_id == cmp.__tag__.local_id and t.__tag__.machine_id == cmp.__tag__.machine_id
end

function _M.initialize(mt, startup_at, local_address)
	epoc = startup_at and tonumber(startup_at) or math.floor(clock.get() * 1000) -- current time in millis
	ffi.metatype('luact_uuid_t', {
		__index = mt.__index, 
		__tostring = function (t)
			return _M.tostring(t)
		end,
		__gc = mt and mt.__gc or function (t)
			idgen:free(t)
		end,
	})
	-- initialize id seed
	local node_address = pulpo.shared_memory('luact_machine_id', function ()
		local v = memory.alloc_typed('uint32_t')
		if local_address then
			v[0] = tonumber(local_address, 16)
		else
			local addr = socket.getifaddr(nil, ffi.defs.AF_INET)
			local af = addr.sa_family
			-- print(addr, addr.sa_family, ffi.defs.AF_INET, ffi.defs.AF_INET6)
			assert(af == ffi.defs.AF_INET, exception.new("invalid", "address", "family", af))
			v[0] = socket.numeric_ipv4_addr_from_sockaddr(addr)
		end
		logger.notice('node_address:', ('%x'):format(v[0]))
		return 'uint32_t', v
	end)
	_M.node_address = node_address[0]
	idgen.seed.__detail__.machine_id = _M.node_address
	idgen.seed.__detail__.thread_id = pulpo.thread_id
	_M.uuid_work.__detail__.machine_id = _M.node_address
	-- TODO : register machine_id/ip address pair to consul.

end

function _M.new()
	if idgen.seed.__detail__.serial >= _M.MAX_SERIAL_ID then
		local current = _M.timestamp(idgen.seed)
		repeat
			clock.sleep(0.01)
			_M.set_timestamp(idgen.seed, msec_timestamp())
		until current ~= _M.timestamp(idgen.seed)
		idgen.seed.__detail__.serial = 0
	else
		idgen.seed.__detail__.serial = idgen.seed.__detail__.serial + 1
	end
	local buf = idgen:new()
	buf.__detail__.serial = idgen.seed.__detail__.serial
	_M.set_timestamp(buf, msec_timestamp())
	if _M.DEBUG then
		logger.info('new uuid:', buf)--, debug.traceback())
	end
	return buf
end
function _M.first(machine_id, thread_id)
	local ret = _M.new()
	if machine_id then
		ret.__detail__.machine_id = machine_id
	end
	if thread_id then
		ret.__detail__.thread_id = thread_id
	end
	_M.set_timestamp(ret, 0)
	ret.__detail__.serial = 0
	return ret
end
function _M.from(ptr)
	return ffi.cast('luact_uuid_t*', ptr)
end
function _M.owner_of(uuid)
	if _M.addr(uuid) == 0 then
		print('invalid addr', debug.traceback())
	end	
	return _M.addr(uuid) == _M.node_address
end
local uuid_work = ffi.new('luact_uuid_t')
_M.uuid_work = uuid_work
function _M.owner_thread_of(uuid_local_id)
	uuid_work.__tag__.local_id = uuid_local_id
	return _M.thread_id(uuid_work) == pulpo.thread_id
end
function _M.serial_from_local_id(uuid_local_id)
	uuid_work.__tag__.local_id = uuid_local_id
	return _M.serial(uuid_work)
end
function _M.from_local_id(uuid_local_id)
	uuid_work.__tag__.local_id = uuid_local_id
	return uuid_work
end
function _M.free(uuid)
	idgen:free(uuid)
end
function _M.valid(uuid)
	return _M.addr(uuid) ~= 0
end
function _M.invalidate(uuid)
	uuid.__detail__.machine_id = 0
end
function _M.debug_create_id(machine_id, thread_id)
	local id = _M.new()
	id.__detail__.thread_id = thread_id or 1
	if type(machine_id) == 'number' then
		id.__detail__.machine_id = machine_id
	else
		local mid
		machine_id:gsub('([0-9]+)%.([0-9]+)%.([0-9]+)%.([0-9]+)', function (_1, _2, _3, _4)
			-- print(_1, _2, _3, _4)
			mid = bit.lshift(tonumber(_1), 24) + bit.lshift(tonumber(_2), 16) + bit.lshift(tonumber(_3), 8) + tonumber(_4)
		end)
		id.__detail__.machine_id = mid
	end
	return id
end

local sprintf_workmem_size = 32
local sprintf_workmem = memory.alloc_typed('char', sprintf_workmem_size)
function _M.tostring(uuid)
	return ('%08x:%08x:%08x'):format(uuid.__tag2__.local_id[0], uuid.__tag2__.local_id[1], uuid.__tag__.machine_id)
end

return _M
