local ffi = require 'ffiex.init'
local luact = require 'luact.init'
local uuid = require 'luact.uuid'
local serde_common = require 'luact.serde.common'

local memory = require 'pulpo.memory'
local lamport = require 'pulpo.lamport'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local tentacle = require 'pulpo.tentacle'

local key = require 'luact.cluster.dht.key'

local _M = {}

exception.define('txn_aborted', { recoverable = true })
exception.define('txn_committed', { recoverable = true })
exception.define('txn_old_retry', { recoverable = true })
exception.define('txn_future_ts', { recoverable = true })
exception.define('txn_push_fail', { recoverable = true })
exception.define('txn_invalid', { recoverable = true })
exception.define('txn_need_retry', { recoverable = true })

-- cdefs
ffi.cdef [[
typedef enum luact_dht_isolation_type {
	SNAPSHOT,
	SERIALIZABLE,
	LINEARIZABLE,
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
	luact_uuid_t id;
	uint32_t priority; //txn priority. lower value == higher priority
	pulpo_hlc_t timestamp; //proposed timestamp
	pulpo_hlc_t start_at; //current txn start timestamp
	pulpo_hlc_t max_ts; //start_at + maximum clock skew
	pulpo_hlc_t last_update;
} luact_dht_txn_t;
]]
assert(ffi.offsetof('luact_dht_txn_t', 'last_update') == 56)
local TXN_STATUS_PENDING = ffi.cast('luact_dht_txn_status_t', 'TXN_STATUS_PENDING')
local TXN_STATUS_ABORTED = ffi.cast('luact_dht_txn_status_t', 'TXN_STATUS_ABORTED')
local TXN_STATUS_COMMITTED = ffi.cast('luact_dht_txn_status_t', 'TXN_STATUS_COMMITTED')
_M.STATUS_PENDING = TXN_STATUS_PENDING
_M.STATUS_ABORTED = TXN_STATUS_ABORTED
_M.STATUS_COMMITTED = TXN_STATUS_COMMITTED

local SNAPSHOT = ffi.cast('luact_dht_isolation_type_t', 'SNAPSHOT')
local SERIALIZABLE = ffi.cast('luact_dht_isolation_type_t', 'SERIALIZABLE')
local LINEARIZABLE = ffi.cast('luact_dht_isolation_type_t', 'LINEARIZABLE')
_M.SNAPSHOT = SNAPSHOT
_M.SERIALIZABLE = SERIALIZABLE
_M.LINEARIZABLE = LINEARIZABLE

_M.DEFAULT_PRIORITY = 1

-- const
-- TxnRetryOptions sets the retry options for handling write conflicts.
local retry_opts = {
	wait = 0.05,
	max_wait = 5,
	wait_multiplier = 2,
	max_attempt = 0, -- infinite
}
_M.retry_opts = retry_opts


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
function txn_mt.new(coord, priority, isolation, debug_opts)
	local p = txn_mt.alloc()
	p:init(coord, priority, isolation, debug_opts)
	return p
end
function txn_mt:init(coord, priority, isolation, debug_opts)
	self.coord = coord
	uuid.invalidate(self.id)
	self.start_at = lamport.ZERO_HLC
	self.timestamp = lamport.ZERO_HLC
	self.max_ts = lamport.ZERO_HLC
	self.isolation = isolation or SERIALIZABLE
	self.status = TXN_STATUS_PENDING
	self.last_update:init()
	self.priority = priority and _M.make_priority(priority) or _M.DEFAULT_PRIORITY
	self.n_retry = 0
	if debug_opts then
		for k,v in pairs(debug_opts) do
			-- print('debug_opts', self, k, v)
			self[k] = v
		end
	end
end
function txn_mt:storage_key()
	return ffi.cast('char *', self.id), ffi.sizeof('luact_uuid_t')
end
function txn_mt:fin()
	--logger.report('txn_mt:fin', self)
	self:invalidate()
	table.insert(txn_mt.cache, self)
end
txn_mt.__gc = txn_mt.fin
function txn_mt:__eq(txn)
	if ffi.cast('void *', txn) == nil then
		return ffi.cast('void *', self) == nil
	end
	return memory.cmp(self, txn, ffi.sizeof('luact_dht_txn_t'))
end
function txn_mt:__len()
	return ffi.sizeof('luact_dht_txn_t')
end
-- [[
function txn_mt:__tostring()
	return ("txn(%s):%s,sts(%s),ts(%s),max_ts(%s),st(%d),n(%d),p(%d)"):format(
		tostring(ffi.cast('void *', self)),
		tostring(self.id),
		tostring(self.start_at),
		tostring(self.timestamp),
		tostring(self.max_ts),
		self.status, self.n_retry, self.priority
	)
end
--]]
function txn_mt:clone(debug_opts)
	if not debug_opts then
		local p = txn_mt.alloc()
		ffi.copy(p, self, ffi.sizeof(self))
		return p
	else
		for _, key in ipairs({"start_at", "timestamp", "status", "max_ts", "n_retry", "last_update", "priority", "id"}) do
			if not debug_opts[key] then
				debug_opts[key] = self[key]
			end
		end
		return txn_mt.new(self.coord, self.priority, self.isolation, debug_opts)
	end
end
function txn_mt:same_origin(txn)
	return txn and uuid.equals(self.id, txn.id)
end
function txn_mt:max_timestamp()
	return self.max_ts
end
function txn_mt:init_timestamp(at)
	local ts = at or _M.clock:issue()
	self.start_at = ts
	self:refresh_timestamp(ts)
end
function txn_mt:refresh_timestamp(ts)
	self.timestamp = ts
	ts:copy_to(self.max_ts)
	self.max_ts:add_walltime(_M.range_manager:max_clock_skew())
end
function txn_mt:valid()
	--print('txn:valid', self, self.coord)
	return uuid.valid(self.id)
end
function txn_mt:uuid()
	return self.id
end
function txn_mt:invalidate()
	uuid.invalidate(self.id)
end
function txn_mt:local_key()
	return ffi.string(self:storage_key())
end
function txn_mt:restart(priority, timestamp)
	self.n_retry = self.n_retry + 1 -- add retry count
	if self.timestamp < timestamp then
		self.timestamp = timestamp
	end
	self.start_at = self.timestamp
	self:upgrade_priority(priority)
	-- logger.report('txn restart', self)
end
function txn_mt:update_with(txn, report_overwrite)
	if not txn then 
		return
	end
	if not self:valid() then
		if report_overwrite then
			logger.report('copy to invalid txn', self, txn)
		end
		ffi.copy(self, txn, ffi.sizeof('luact_dht_txn_t'))
		return
	end
	if txn.status ~= TXN_STATUS_PENDING then
		self.status = txn.status
	end
	if self.n_retry < txn.n_retry then
		self.n_retry = txn.n_retry
	end
	if self.timestamp < txn.timestamp then
		self.timestamp = txn.timestamp
	end
	if self.start_at < txn.start_at then
		self.start_at = txn.start_at
	end
	if self.priority < txn.priority then
		self.priority = txn.priority
	end
	self.max_ts = txn.max_ts
end
function txn_mt:upgrade_priority(minimum_priority)
	if minimum_priority > self.priority then
		self.priority = minimum_priority
	end
end
function txn_mt:debug_set_priority(priority)
	self.priority = priority
end
ffi.metatype('luact_dht_txn_t', txn_mt)
serde_common.register_ctype('struct', 'luact_dht_txn', nil, serde_common.LUACT_DHT_TXN)



-- txn coordinator
local txn_coord_mt = {}
txn_coord_mt.__index = txn_coord_mt
txn_coord_mt.range_work1 = memory.alloc_typed('luact_dht_key_range_t')
txn_coord_mt.range_work2 = memory.alloc_typed('luact_dht_key_range_t')
function txn_coord_mt:initialize()
end
function txn_coord_mt:__actor_destroy__()
end
function txn_coord_mt:start(txn)
	txn.id = uuid.new()
	txn:init_timestamp()
	-- logger.info('txn_coord_start', txn)
end
function txn_coord_mt:heartbeat(txn)
	while true do
		local ok, r = pcall(_M.range_manager.heartbeat_txn, _M.range_manager, txn)
		if ok then -- transaction finished by other process (maybe max timestamp exceed)
			if r.status ~= TXN_STATUS_PENDING then
				logger.info('txn seems finished', r)
				ok, r = pcall(_M.resolve_version, r, nil, false, true)
				if ok then
					return -- stop hearbbeat for this txn
				else
					logger.error('resolve txn fails', r)
				end
			end
		else
			logger.error('heartbeat_txn error', txn, r)
		end
		luact.clock.sleep(_M.range_manager.opts.txn_heartbeat_interval)
	end
end
function txn_coord_mt:start_heartbeat(txn)
	return tentacle(self.heartbeat, self, txn)
end
-- resolve all transactional state and finish it.
-- causion : this logic may execute on different node from calling end_txn.
function txn_coord_mt:finish(txn, exist_txn, commit)
-- logger.report('txn_coord_mt:finish', commit, exist_txn)
	local reply
	if exist_txn then
		-- Use the persisted transaction record as final transaction.
		reply = exist_txn
		if reply.status == TXN_STATUS_COMMITTED then
			exception.raise('txn_committed', reply)
		elseif reply.status == TXN_STATUS_ABORTED then
			exception.raise('txn_aborted', reply)
		elseif txn.n_retry < reply.n_retry then
			exception.raise('txn_old_retry', txn.n_retry, reply.n_retry, reply)
		elseif reply.timestamp < txn.start_at then
			-- The transaction record can only ever be pushed forward, so it's an
			-- error if somehow the transaction record has an earlier timestamp
			-- than the transaction timestamp.
			exception.raise('txn_future_ts', txn.start_at, reply.timestamp, reply)
		end
		-- Take max of requested epoch and existing epoch. The requester
		-- may have incremented the epoch on retries.
		if reply.n_retry < txn.n_retry then
			reply.n_retry = txn.n_retry
		end
		-- Take max of requested priority and existing priority. This isn't
		-- terribly useful, but we do it for completeness.
		if reply.priority < txn.priority then
			reply.priority = txn.priority
		end
	else
		reply = txn
	end	
	-- Set transaction status to COMMITTED or ABORTED as per the
	-- args.Commit parameter.
	if commit then
		-- If the isolation level is SERIALIZABLE, return a transaction
		-- retry error if the commit timestamp isn't equal to the txn
		-- timestamp.
		-- logger.info('iso', txn.isolation == SERIALIZABLE, reply.timestamp, txn.start_at)
		if txn.isolation == SERIALIZABLE and reply.timestamp ~= txn.start_at then
			exception.raise('txn_need_retry', reply)
		end
		reply.status = TXN_STATUS_COMMITTED
	else
		reply.status = TXN_STATUS_ABORTED
	end

	return reply
end
function txn_coord_mt:resolve_version(txn, commit, sync, keep_hb)
	if commit ~= nil then -- force set txn status
		txn.status = commit and TXN_STATUS_COMMITTED or TXN_STATUS_ABORTED
	end
	local key = txn:local_key()	
	local txn_data = self.txns[key]
	if not keep_hb then
		tentacle.cancel(txn_data.th) -- stop heartbeat
	end
	self.txns[key] = nil
	local ranges = {}
	-- get ranges to send end_txn. (de-dupe is done in txn_coord_mt:add_cmd)
	-- currently key and end_key of single command should be within one range object.
	-- that means, if we implement some extensional command like SQL query on this dht system, 
	-- command of the range should be devided into per-range basis by command invoker. 
	-- TODO : should we support the case where (key(), end_key()) covers multi-range?
	local evs = {}
	for i=1,#txn_data do
		-- commit each range.
		local c = txn_data[i]
		table.insert(evs, tentacle(function (rm, _txn, k, kl, kind, ek, ekl)
			local rng = rm:find(k, kl, kind)
			-- local ekstr = ek and ffi.string(ek, ekl) or "[empty]"
			-- logger.info('rng:resolve', ffi.string(k, kl), ekstr)
			local n = rng:resolve(_txn, 0, k, kl, ek, ekl, _txn.timestamp) -- 0 means all records in the range
			-- logger.info('rng:resolve end', n, 'processed between', ffi.string(k, kl), ekstr)
		end, _M.range_manager, txn, c:key(), c:keylen(), c.kind, c:end_key(), c:end_keylen()))
	end
	if sync then
		logger.info('wait all resolve completion')
		event.join(nil, unpack(evs))
	else
		return evs
	end
end
function txn_coord_mt:add_cmd(txn, cmd)
	local k = txn:local_key()
	local txn_data = self.txns[k]
	if not txn_data then
		local th = self:start_heartbeat(txn)
		assert(th, "thread create fails")
		txn_data = {txn = txn, th = th}
		self.txns[k] = txn_data
	end
	local keyrng, kr = self.range_work1, self.range_work2
	keyrng:init(cmd:key(), cmd:keylen(), cmd:end_key(), cmd:end_keylen())
	local ridx = {}
	-- de-dupe ranges
	for i=#txn_data,1,-1 do
		local c = txn_data[i]
		kr:init(c:key(), c:keylen(), c:end_key(), c:end_keylen())
		if keyrng:contains_range(kr) then
			table.remove(txn_data, i)
		elseif kr:contains_range(keyrng) then
			return -- because kr has already removed all range which keyrng can remove.
		end
	end
	table.insert(txn_data, cmd)
end


-- module functions
local default_opts = {
	linearizable = false, -- no linearizability
	batch_size = 100,
}
function _M.initialize(rm, opts)
	_M.range_manager = rm
	_M.clock = rm.clock
	_M.opts = util.merge_table(default_opts, opts or {})
	if _M.coord_actor then
		luact.kill(_M.coord_actor)
		_M.coord_actor = nil
	end
	_M.coordinator = setmetatable({
		opts = _M.opts,
		txns = {},
	}, txn_coord_mt)
	_M.coordinator:initialize()
	_M.coord_actor = luact(_M.coordinator)
end
function _M.new_txn(priority, isolation, txn)
	if txn then
		txn:init(_M.coord_actor, priority, isolation)
	else
		txn = txn_mt.new(_M.coord_actor, priority, isolation)
	end
	return txn
end
function _M.start_txn(txn)
	if txn and (not uuid.valid(txn.id)) then
		_M.coordinator:start(txn)
		assert(uuid.valid(txn.id))
		return true
	end
end
function _M.run_txn(opts, fn, ...)
	return util.retry(retry_opts, function (ptxn, on_commit, proc, ...)
		local txn = ptxn[1]
		txn.status = TXN_STATUS_PENDING
		local ok, r = pcall(proc, txn, ...)
		if not ok then
			-- logger.report('run_Txn error', r)
		end
		if ok then
			if txn.status == TXN_STATUS_PENDING then
				_M.range_manager:end_txn(txn, true)
				if on_commit then on_commit(...) end
			end
			return util.retry_pattern.STOP
		elseif type(r) == 'string' then
			logger.warn('txn aborted by error', r)
			_M.range_manager:end_txn(txn, false)
			return util.retry_pattern.ABORT			
		elseif r:is('actor_no_body') then
			return util.retry_pattern.CONTINUE
		elseif r:is('txn_ts_uncertainty') then
			return util.retry_pattern.RESTART
		elseif r:is('txn_aborted') then
			-- create new transaction
			ptxn[1] = _M.new_txn(txn.priority, txn.isolation) 
			return util.retry_pattern.CONTINUE
		elseif r:is('txn_push_fail') then
			return util.retry_pattern.CONTINUE
		elseif r:is('txn_need_retry') then
			return util.retry_pattern.RESTART
		end
		logger.warn('txn aborted by error', r)
		_M.range_manager:end_txn(txn, false)
		return util.retry_pattern.ABORT
	end, {_M.new_txn(opts.priority, opts.isolation)}, opts.on_commit, fn, ...)
end
function _M.resolve_version(txn, commit, sync, keep_hb)
	_M.coordinator:resolve_version(txn, commit, sync, keep_hb)
end
function _M.end_txn(txn, exist_txn, commit)
	return _M.coordinator:finish(txn, exist_txn, commit)
end
function _M.add_cmd(txn, cmd, kind)
	_M.coordinator:add_cmd(txn, cmd)
end
function _M.storage_key(txn)
	-- TODO : cockroach use first key which handled by this txn, as prefix. is it effective?
	-- because UUID itself is enough for identification of each transaction.
	-- local c = _M.coordinator.txns[txn:local_key()]
	-- print('c', c, #c)
	-- if (not c) or (#c < 1) then return nil end
	-- local k = ffi.string(c:key(), c:keylen())..txn:storage_key()
	-- return k, #k
	if not txn then
		logger.error('txn null')
	end
	return txn:storage_key()
end
-- MakePriority generates a random priority value, biased by the
-- specified userPriority. If userPriority=100, the resulting
-- priority is 100x more likely to be probabilistically greater
-- than a similar invocation with userPriority=1.
function _M.make_priority(user_priority)
	-- A currently undocumented feature allows an explicit priority to
	-- be set by specifying priority < 1. The explicit priority is
	-- simply -userPriority in this case. This is hacky, but currently
	-- used for unittesting. Perhaps this should be documented and allowed.
	if user_priority < 0 then
		return -user_priority
	end
	if user_priority == 0 then
		user_priority = 1
	end
	-- The idea here is to bias selection of a random priority from the
	-- range [1, 2^31-1) such that if userPriority=100, it's 100x more
	-- likely to be a higher int32 than if userPriority=1. The formula
	-- below chooses random values according to the following table:
	--   userPriority  |  range
	--   1             |  all positive int32s
	--   10            |  top 9/10ths of positive int32s
	--   100           |  top 99/100ths of positive int32s
	--   1000          |  top 999/1000ths of positive int32s
	--   ...etc
	return 0xFFFFFFFF - math.random(0xFFFFFFFF/user_priority)
end

function _M.debug_make_txn(debug_opts)
	return txn_mt.new(_M.coord_actor, nil, nil, debug_opts)[0]
end

return _M