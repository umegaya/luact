local ffi = require 'ffiex.init'
local luact = require 'luact.init'
local uuid = require 'luact.uuid'

local memory = require 'pulpo.memory'
local lamport = require 'pulpo.lamport'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'

local _M = {}

-- cdefs
ffi.cdef [[
typedef enum luact_dht_isolation_type {
	SNAPSHOT_ISOLATION,
	SERIALIZED_SNAPSHOT_ISOLATION,
	LINEARIZABILITY,
} luact_dht_isolation_type_t;

typedef enum luact_dht_txn_status {
	TXN_STATUS_PENDING,
	TXN_STATUS_ABORTED,
	TXN_STATUS_COMMITTED,
} luact_dht_txn_status_t;

typedef struct luact_dht_txn {
	luact_uuid_t coord;
	uint16_t n_retry;
	uint8_t status, isolation;
	pulpo_hlc_t timestamp; //proposed timestamp
	pulpo_hlc_t start_at; //initial timestamp
	pulpo_hlc_t max_ts; //start_at + maximum clock skew
} luact_dht_txn_t;
]]
local TXN_STATUS_PENDING = ffi.cast('luact_dht_txn_status_t', 'TXN_STATUS_PENDING')
local TXN_STATUS_ABORTED = ffi.cast('luact_dht_txn_status_t', 'TXN_STATUS_ABORTED')
local TXN_STATUS_COMMITTED = ffi.cast('luact_dht_txn_status_t', 'TXN_STATUS_COMMITTED')
_M.STATUS_PENDING = TXN_STATUS_PENDING
_M.STATUS_ABORTED = TXN_STATUS_ABORTED
_M.STATUS_COMMITTED = TXN_STATUS_COMMITTED

local SERIALIZED_SNAPSHOT_ISOLATION = ffi.cast('luact_dht_isolation_type_t', 'SERIALIZED_SNAPSHOT_ISOLATION')
local SNAPSHOT_ISOLATION = ffi.cast('luact_dht_isolation_type_t', 'SNAPSHOT_ISOLATION')
local LINEARIZABILITY = ffi.cast('luact_dht_isolation_type_t', 'LINEARIZABILITY')
_M.SERIALIZED_SNAPSHOT_ISOLATION = SERIALIZED_SNAPSHOT_ISOLATION
_M.SNAPSHOT_ISOLATION = SNAPSHOT_ISOLATION
_M.LINEARIZABILITY = LINEARIZABILITY

-- const
-- TxnRetryOptions sets the retry options for handling write conflicts.
local retry_opts = {
	wait = 0.05,
	max_wait = 5,
	wait_multiplier = 2,
	max_attempt = 0, -- infinite
}


-- txn
local txn_mt = {}
txn_mt.__index = txn_mt
txn_mt.cache = {}
function txn_mt.alloc()
	if #txn_mt.cache > 0 then
		return table.remove(txn_mt.cache)
	end
	return memory.alloc_typed('luact_dht_txn_t')
end
function txn_mt.new(coord, isolation, debug_opts)
	local p = txn_mt.alloc()
	p:init(coord, isolation, debug_opts)
	return p
end
function txn_mt:init(coord, isolation, debug_opts)
	local ts = _M.clock:issue()
	self.coord = coord
	self.start_at = ts
	self:refresh_timestamp(ts)
	self.isolation = isolation or SERIALIZED_SNAPSHOT_ISOLATION
	self.status = TXN_STATUS_PENDING
	self.n_retry = 0
	if debug_opts then
		for k,v in pairs(debug_opts) do
			-- print('debug_opts', self, k, v)
			self[k] = v
		end
	end
end
function txn_mt:fin()
	self:invalidate()
	table.insert(txn_mt.cache, self)
end
txn_mt.__gc = txn_mt.fin
function txn_mt:__eq(txn)
	return memory.cmp(self, txn, ffi.sizeof('luact_dht_txn_t'))
end
function txn_mt:__len()
	return ffi.sizeof('luact_dht_txn_t')
end
function txn_mt:__tostring()
	return ("txn(%s):%s:%s,ts(%s),max_ts(%s),st(%d),n(%d)"):format(
		tostring(ffi.cast('void *', self)),
		tostring(self.coord),
		tostring(self.start_at),
		tostring(self.timestamp),
		tostring(self.max_ts),
		self.status, self.n_retry
	)
end
function txn_mt:clone(debug_opts)
	if not debug_opts then
		local p = txn_mt.alloc()
		ffi.copy(p, self, ffi.sizeof(self))
		return p
	else
		for _, key in ipairs({"start_at", "timestamp", "status", "max_ts", "n_retry"}) do
			if not debug_opts[key] then
				debug_opts[key] = self[key]
			end
		end
		return txn_mt.new(self.coord, self.isolation, debug_opts)
	end
end
function txn_mt:same_origin(txn)
	return uuid.equals(self.coord, txn.coord) and (self.start_at == txn.start_at)
end
function txn_mt:max_timestamp()
	return self.max_ts
end
function txn_mt:refresh_timestamp(at)
	local ts = at or _M.clock:issue()
	self.timestamp = ts
	self.max_ts = ts:add_walltime(_M.opts.max_clock_skew)
end
function txn_mt:valid()
	--print('txn:valid', self, self.coord)
	return uuid.valid(self.coord)
end
function txn_mt:invalidate()
	uuid.invalidate(self.coord)
end
function txn_mt:as_key()
	return self.start_at:as_byte_string()
end
--[[
func (t *Transaction) Restart(userPriority, upgradePriority int32, timestamp Timestamp) {
	t.Epoch++
	if t.Timestamp.Less(timestamp) {
		t.Timestamp = timestamp
	}
	// Set original timestamp to current timestamp on restart.
	t.OrigTimestamp = t.Timestamp
	// Potentially upgrade priority both by creating a new random
	// priority using userPriority and considering upgradePriority.
	t.UpgradePriority(MakePriority(userPriority))
	t.UpgradePriority(upgradePriority)
}

// Update ratchets priority, timestamp and original timestamp values (among
// others) for the transaction. If t.ID is empty, then the transaction is
// copied from o.
func (t *Transaction) Update(o *Transaction) {
	if o == nil {
		return
	}
	if len(t.ID) == 0 {
		*t = *gogoproto.Clone(o).(*Transaction)
		return
	}
	if o.Status != PENDING {
		t.Status = o.Status
	}
	if t.Epoch < o.Epoch {
		t.Epoch = o.Epoch
	}
	if t.Timestamp.Less(o.Timestamp) {
		t.Timestamp = o.Timestamp
	}
	if t.OrigTimestamp.Less(o.OrigTimestamp) {
		t.OrigTimestamp = o.OrigTimestamp
	}
	// Should not actually change at the time of writing.
	t.MaxTimestamp = o.MaxTimestamp
	// Copy the list of nodes without time uncertainty.
	t.CertainNodes = NodeList{Nodes: append(Int32Slice(nil),
		o.CertainNodes.Nodes...)}
	t.UpgradePriority(o.Priority)
}

// UpgradePriority sets transaction priority to the maximum of current
// priority and the specified minPriority.
func (t *Transaction) UpgradePriority(minPriority int32) {
	if minPriority > t.Priority {
		t.Priority = minPriority
	}
}
]]
ffi.metatype('luact_dht_txn_t', txn_mt)



-- txn coordinator
local txn_coord_mt = {}
txn_coord_mt.__index = txn_coord_mt
function txn_coord_mt:start(txn)
	local k = txn:as_key()
	if self.txns[k] then
		exception.raise('fatal', 'txn already exists', txn.start_at)
	end
	self.txns[k] = {txn = txn, keys = {}}
end
function txn_coord_mt:heartbeat()
end
function txn_coord_mt:finish(txn, commit)
	txn.status = commit and TXN_STATUS_COMMITTED or TXN_STATUS_ABORTED
	local key = txn:as_key()	
	local txn_data = self.txns[key]
	local ranges = {}
	-- get ranges to send end_txn. (with de-dupe)
	for i=1,#txn_data do
		-- commit each range.
		local k, kind = unpack(txn_data[i])
		local rng = _M.range_manager:find(k, #k, kind)
		local span = ranges[rng]
		if not span then
			ranges[rng] = { min = k, max = k }
		elseif memory.rawcmp_ex(span.max, #span.max, k, #k) < 0 then
			ranges[rng].max = k
		elseif memory.rawcmp_ex(span.min, #span.min, k, #k) > 0 then
			ranges[rng].min = k
		end 
	end
	for rng,span in pairs(ranges) do
		rng:resolve(txn, 0, span.min, #span.min, span.max, #span.max)
	end
end
function txn_coord_mt:add_key(start_at, key, kind)
	local txn_data = self.txns[start_at:as_byte_string()]
	table.insert(txn_data, {key, kind})
end



-- module functions
local default_opts = {
	max_clock_skew = 0.25, -- 250 msec
}
function _M.initialize(rm, opts)
	_M.range_manager = rm
	_M.clock = rm.clock
	_M.opts = util.merge_table(default_opts, opts or {}) 
	_M.coordinator = setmetatable({
		txns = {},
	}, txn_coord_mt)
	_M.coord_actor = luact(_M.coordinator)
end
function _M.new_txn(isolation)
	local txn = txn_mt.new(_M.coord_actor, isolation)
	_M.coordinator:start(txn)
	return txn
end
function _M.fin_txn(txn, commit)
	_M.coordinator:finish(txn, commit)
	txn:fin()
end
function _M.run_txn(txn, proc, ...)
	_M.fin_txn(txn, util.retry(retry_opts, function (txn, proc, ...)
		local ok, r = pcall(proc, txn, ...)
		if ok then
			return 
		elseif r:is('mvcc') then
			if r.args[1] == 'txn_ts_uncertainty' then
				txn:refresh_timestamp()
				return util.retry_pattern.RESTART
			end
		end
		return util.retry_pattern.TERMINATE
	end))
end
function _M.debug_make_txn(debug_opts)
	return txn_mt.new(_M.coord_actor, nil, debug_opts)[0]
end

return _M