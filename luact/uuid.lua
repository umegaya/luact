local _M = {}

-- constant
_M.THREAD_BIT_SIZE = 12
_M.TIMESTAMP_BIT_SIZE = 42
_M.SERIAL_BIT_SIZE = 64 - (_M.THREAD_BIT_SIZE + _M.TIMESTAMP_BIT_SIZE)
_M.MAX_SERIAL_ID = (bit.lshift(1, _M.SERIAL_BIT_SIZE) - 1)

-- cdefs
ffi.cdef(([[
typedef union luact_uuid {
	struct luact_id_detail {	//luact implementation detail
		uint64_t thread_id:%d; 	//upto 4k core
		uint64_t timestamp:%d;	//42bit msec timestamp 
		uint64_t serial:%d; 	//can generate 1024 actor id/msec/thread
		uint32_t machine_id;	//cluster local ip address
	} detail;
	struct luact_id_tag {
		uint64_t local_id;
		uint32_t machine_id;
	} tag;
} luact_id_t;
]]):format(_M.THREAD_BIT_SIZE, _M.TIMESTAMP_BIT_SIZE, _M.SERIAL_BIT_SIZE))

-- vars
local idgen = {
	seed = ffi.new('luact_uuid_t'), free = {}
}
local epoc
local sleeper

-- local functions
local function msec_timestamp()
	return (math.floor(pulpo.util.clock() * 1000) - epoc)
end

-- module function 
function _M.initialize(mt, startup_at, local_address)
	epoc = (startup_at and tonumber(startup_at) or math.floor(pulpo.util.clock() * 1000))
	sleeper = pulpo.task.new(0.01, 0.1)
	ffi.metatype('luact_uuid_t', {
		__index = setmetatable({
			__timestamp = function (t) return (epoc + t.data.timestamp) / 1000 end,
			__thread_id = function (t) return t.data.thread_id end,
			__local_id = function (t) return t.tag.local_id end,
			__addr = function (t) return t.tag.addr end,
		}, mt)	
	})
	-- initialize id seed
	_M.mid_ptr = pulpo.shared_memory('luact_machine_id', function ()
		local v = memory.alloc_typed('uint32_t')
		if local_address then
			v[0] = tonumber(local_address, 16)
		else
			local addr = pulpo.socket.getifaddr()
			assert(addr.sa_family == ffi.defs.IF_INET, "invalid address family:"..addr.sa_family)
			v[0] = ffi.cast('struct sockaddr_in*', addr).sin_addr.in_addr
		end
		return 'uint32_t', v
	end)
	idgen.seed.detail.machine_id = _M.mid_ptr[0]
	idgen.seed.detail.thread_id = pulpo.thread_id
	-- TODO : register machine_id/ip address pair to consul.
end

function _M.new()
	if idgen.seed.detail.serial >= _M.MAX_SERIAL_ID then
		local current = idgen.seed.detail.timestamp
		repeat
			sleeper.sleep(0.01)
			idgen.seed.detail.timestamp = msec_timestamp()
		until current ~= idgen.seed.detail.timestamp
		idgen.seed.detail.serial = 0
	else
		idgen.seed.detail.serial = idgen.seed.detail.serial + 1
	end
	local buf
	if #idgen.free > 0 then
		buf = table.remove(idgen.free)
	else
		buf = memory.alloc_typed('luact_uuid_t')
		buf.detail.machine_id = idgen.seed.detail.machine_id
		buf.detail.thread_id = idgen.seed.detail.thread_id
	end
	buf.detail.serial = idgen.seed.detail.serial
	buf.detail.timestamp = msec_timestamp()
	return buf
end
function _M.free(uuid)
	table.insert(idgen.free, uuid)
end

return _M
