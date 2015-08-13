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
	uint16_t kl, klimit;
	uint8_t kind, key[0];
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
function txn_mt.size(kl)
	return ffi.sizeof('luact_dht_txn_t') + kl
end
local alloc_stacks = {}
-- TODO : if allocation bug almost gone, give reasonable initial key buffer size to avoid realloc
local DEFAULT_KEY_BUF_SIZE = 16
function txn_mt.alloc(kl)
	if (not kl) or (kl < DEFAULT_KEY_BUF_SIZE) then
		kl = DEFAULT_KEY_BUF_SIZE
	end
	assert(kl < 65536)
	local p
	if #txn_mt.cache > 0 then
		p = table.remove(txn_mt.cache)
		if kl and p.klimit < kl then
			local tmp = memory.realloc(p, txn_mt.size(kl))
			if not tmp then
				exception.raise('fatal', 'malloc', txn_mt.size(kl))
			end
			if p ~= tmp then
				ffi.gc(p, nil)
			end
			p = ffi.cast('luact_dht_txn_t *', tmp)
			p.klimit = kl
			ffi.gc(p, txn_mt.fin)
		end
	else
		p = ffi.cast('luact_dht_txn_t *', memory.alloc(txn_mt.size(kl)))
		p.klimit = kl
		ffi.gc(p, txn_mt.fin)
		-- logger.warn('newly create', p)
	end
	return p
end
function txn_mt.new(coord, priority, isolation, debug_opts)
	local p = txn_mt.alloc()
	p:init(coord, priority, isolation, debug_opts)
	if debug_opts and debug_opts.key then
		local k = debug_opts.key
		p = p:set_key_and_kind(debug_opts.kind, k, #k)
	end
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
	self.kl = 0
	self.kind = 0
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
	-- logger.report('txn_mt:fin', self)
	self:invalidate()
	table.insert(txn_mt.cache, self)
end
function txn_mt:__eq(txn)
	if ffi.cast('void *', txn) == nil then
		return ffi.cast('void *', self) == nil
	end
	return (#self == #txn) and memory.cmp(self, txn, #self)
end
function txn_mt:__len()
	return txn_mt.size(self.kl)
end
-- [[
function txn_mt:__tostring()
	return ("txn(%s):%s,sts(%s),ts(%s),max_ts(%s),st(%d),n(%d),p(%d),k(%d:%s),%d/%d/%d"):format(
		tostring(ffi.cast('void *', self)),
		tostring(self.id),
		tostring(self.start_at),
		tostring(self.timestamp),
		tostring(self.max_ts),
		self.status, self.n_retry, self.priority,
		tonumber(self.kind), ffi.string(self.key, self.kl), self.kl, self.klimit, #self
	)
end
--]]
function txn_mt:clone(debug_opts)
	if not debug_opts then
		local p = txn_mt.alloc(self.kl)
		ffi.copy(p, self, #self)
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
function txn_mt:set_key_and_kind(kind, k, kl)
	if self.kind ~= 0 then
		-- already key and kind is set
		return self
	end
	if self.klimit < kl then
		local tmp = txn_mt.alloc(kl)
		local klimit = tmp.klimit
		ffi.copy(tmp, self, #self)
		tmp.klimit = klimit
		self = tmp
	end
	self.kind = kind
	self.kl = kl
	ffi.copy(self.key, k, kl)
	return self
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
ffi.metatype('luact_dht_txn_t', txn_mt)
serde_common.register_ctype('struct', 'luact_dht_txn', {
	msgpack = {
		packer = function (pack_procs, buf, ctype_id, obj, length)
			local used = #obj
			local p, ofs = pack_procs.pack_ext_cdata_header(buf, used, ctype_id)
			buf:reserve(used)
			ffi.copy(p + ofs, obj, used)
			return ofs + used
		end,
		unpacker = function (rb, len)
			local ptr = txn_mt.alloc(len - ffi.sizeof('luact_dht_txn_t'))
			ffi.copy(ptr, rb:curr_byte_p(), len)
			rb:seek_from_curr(len)
			return ptr
		end,
	},
}, serde_common.LUACT_DHT_TXN)
-- serde_common.register_ctype('struct', 'luact_dht_txn', nil, serde_common.LUACT_DHT_TXN)



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
	return txn
end
function txn_coord_mt:heartbeat(wtxn)
	-- logger.info('coor heartbeat start', wtxn)
	while true do
		-- logger.info('start hb', wtxn:strip())
		local ok, r = pcall(_M.range_manager.heartbeat_txn, _M.range_manager, wtxn)
		-- logger.info('end hb', wtxn:strip(), ok, r)
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
			logger.error('heartbeat_txn error', wtxn:strip(), r)
		end
		luact.clock.sleep(_M.range_manager.opts.txn_heartbeat_interval)
	end
end
function txn_coord_mt:start_heartbeat(wtxn)
	return tentacle(self.heartbeat, self, wtxn)
end
-- resolve all transactional state and finish it.
-- causion : this logic may execute on different node from calling end_txn.
function txn_coord_mt:finish(txn, exist_txn, commit)
-- logger.report('txn_coord_mt:finish', commit, exist_txn, txn)
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
			-- logger.info('isolation is SERIALIZABLE and timestamp move fwd', reply.timestamp, txn.start_at)
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
	-- get ranges to send end_txn. (de-dupe is done in txn_coord_mt:add_cmd)
	-- currently key and end_key of single command should be within one range object.
	-- that means, if we implement some extensional command like SQL query on this dht system, 
	-- command of the range should be devided into per-range basis by command invoker. 
	-- TODO : should we support the case where (key(), end_key()) covers multi-range?
	local evs = {}
	for i=1,#txn_data do
		-- commit each range.
		local c = txn_data[i]
		table.insert(evs, tentacle(function (rm, wtxn, ts, k, kl, kind, ek, ekl)
			-- local ekstr = ek and ffi.string(ek, ekl) or "[empty]"
			-- logger.info('rng:resolve', ffi.string(k, kl), ekstr)
			local n = rm:resolve(wtxn, 0, k, kl, ek, ekl, ts) -- 0 means all records in the range
			-- logger.info('rng:resolve end', n, 'processed between', ffi.string(k, kl), ekstr)
		end, _M.range_manager, _M.wrap(txn), txn.timestamp, c:key(), c:keylen(), c.kind, c:end_key(), c:end_keylen()))
	end
	if sync then
		logger.info('wait all resolve completion')
		event.join(nil, unpack(evs))
	else
		return evs
	end
end
function txn_coord_mt:add_cmd(wtxn, cmd)
	local txn = wtxn:strip()
	local k = txn:local_key()
	local txn_data = self.txns[k]
	if not txn_data then
		local th = self:start_heartbeat(wtxn)
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



-- module function helper
local function new_txn(priority, isolation, txn)
	if txn then
		txn:init(_M.coord_actor, priority, isolation)
	else
		txn = txn_mt.new(_M.coord_actor, priority, isolation)
	end
	return txn
end
local function abort_txn(wtxn)
	if wtxn:strip():valid() then
		_M.range_manager:end_txn(wtxn, false)
	end
end
local wrap_txn_mt = {}
wrap_txn_mt.__index = wrap_txn_mt
function wrap_txn_mt:strip()
	return self[1]
end
function wrap_txn_mt:set_raw_txn(txn)
	self[1] = txn
end
function wrap_txn_mt:setup_and_strip(kind, k, kl)
	self[1] = self[1]:set_key_and_kind(kind, k, kl)
	return self[1]
end
function wrap_txn_mt:debug_set_priority(prio)
	self[1].priority = prio
end
local function wrap_txn(txn)
	return setmetatable({txn}, wrap_txn_mt)
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
function _M.start_txn(txn)
	if txn and (not uuid.valid(txn.id)) then
		txn = _M.coordinator:start(txn)
		assert(uuid.valid(txn.id))
		return txn
	end
end
function _M.run_txn(opts_or_fn, ...)
	local opts, fn, args_index
	if type(opts_or_fn) == 'function' then
		fn = opts_or_fn
		opts = {}
		args_index = 1
	elseif type(opts_or_fn) == 'table' then
		opts = opts_or_fn
		fn = select(1, ...)
		args_index = 2
	end
	return util.retry(retry_opts, function (wtxn, on_commit, proc, ...)
		--logger.info('on_commit', on_commit)
		wtxn:strip().status = TXN_STATUS_PENDING
		local ok, r = pcall(proc, wtxn, ...) 
		-- proc may change raw txn ptr
		txn = wtxn:strip()
		if ok then
			if txn.status == TXN_STATUS_PENDING then
				local cmd_on_commit = on_commit and on_commit(txn, ...)
				ok, r = pcall(_M.range_manager.end_txn, _M.range_manager, wtxn, true, cmd_on_commit)
				if ok then 
					return util.retry_pattern.STOP 
				end
				-- logger.report('run_txn: end_txn error', r)
				-- fall through to error handling
			else
				return util.retry_pattern.STOP
			end
		else
			-- logger.report('run_txn: proc error', r)
		end
		if type(r) == 'string' then
			-- logger.warn('txn aborted by error', r)
			abort_txn(wtxn)
			return util.retry_pattern.ABORT			
		elseif r:is('actor_no_body') then
			return util.retry_pattern.CONTINUE
		elseif r:is('txn_ts_uncertainty') then
			return util.retry_pattern.RESTART
		elseif r:is('txn_aborted') then
			-- create new transaction
			wtxn:set_raw_txn(new_txn(txn.priority, txn.isolation))
			return util.retry_pattern.CONTINUE
		elseif r:is('txn_push_fail') then
			return util.retry_pattern.CONTINUE
		elseif r:is('txn_need_retry') then
			return util.retry_pattern.RESTART
		end
		-- logger.warn('txn aborted by error', r)
		abort_txn(wtxn)
		return util.retry_pattern.ABORT
	end, wrap_txn(new_txn(opts.priority, opts.isolation)), opts.on_commit, fn, select(args_index, ...))
end
function _M.resolve_version(txn, commit, sync, keep_hb)
	_M.coordinator:resolve_version(txn, commit, sync, keep_hb)
end
function _M.wrap(txn)
	return wrap_txn(txn)
end
function _M.end_txn(txn, exist_txn, commit)
	return _M.coordinator:finish(txn, exist_txn, commit)
end
function _M.add_cmd(txn, cmd, kind)
	_M.coordinator:add_cmd(txn, cmd)
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
	if not debug_opts.id then
		debug_opts.id = uuid.new()
	end
	return txn_mt.new(debug_opts.coord or _M.coord_actor, nil, nil, debug_opts)
end

return _M