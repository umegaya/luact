local luact = require 'luact.init'
local uuid = require 'luact.uuid'
local clock = require 'luact.clock'
local serde = require 'luact.serde'
local serde_common = require 'luact.serde.common'
local router = require 'luact.router'
local actor = require 'luact.actor'

local pulpo = require 'pulpo.init'
local event = require 'pulpo.event'
local util = require 'pulpo.util'
local memory = require 'pulpo.memory'
local socket = require 'pulpo.socket'
local tentacle = require 'pulpo.tentacle'
local exception = require 'pulpo.exception'
local lamport = require 'pulpo.lamport'
local fs = require 'pulpo.fs'

local raft = require 'luact.cluster.raft'
local gossip = require 'luact.cluster.gossip'
local key = require 'luact.cluster.dht.key'
local cache = require 'luact.cluster.dht.cache'
local cmd = require 'luact.cluster.dht.cmd'
local scanner = require 'luact.cluster.dht.scanner'

local mvcc = require 'luact.storage.mvcc'
local txncoord = require 'luact.storage.txncoord'


-- module share variable
local _M = {}
local range_manager


-- constant
_M.DEFAULT_REPLICA = 3
_M.META1_FAMILY_NAME = '__dht.meta1__'
_M.META2_FAMILY_NAME = '__dht.meta2__'
_M.DATA_FAMILY_NAME = '__dht.data__'
_M.META2_MAX_KEY = memory.alloc_typed('luact_dht_key_t')[0]
_M.META2_MAX_KEY:init(string.char(0xFF), 1)
_M.META2_SCAN_END_KEYSTR = string.char(0xFF, 0xFF)
-- data category
_M.KIND_META1 = 1
_M.ROOT_METADATA_KIND = _M.KIND_META1
_M.KIND_META2 = 2
_M.NON_METADATA_KIND_START = _M.KIND_META2 + 1
_M.KIND_TXN = 3
_M.KIND_VID = 4
_M.KIND_STATE = 5
_M.BUILTIN_KIND_START = 1
_M.BUILTIN_KIND_END = 5
-- user kind
_M.USER_KIND_PREFIX_MIN = 0x40
_M.USER_KIND_PREFIX_MAX = 0xfe
_M.NUM_USER_KIND = _M.USER_KIND_PREFIX_MAX - _M.USER_KIND_PREFIX_MIN
-- syskey prefix
_M.SYSKEY_CATEGORY_KIND = 0
-- hook type
_M.SPLIT_HOOK = 1
_M.MERGE_HOOK = 2
-- resolve txn type
_M.RESOLVE_TXN_CLEANUP = 1 -- cleanup txn. eg. gc?
_M.RESOLVE_TXN_ABORT = 2 -- abort txn. eg. write/write conflict
_M.RESOLVE_TXN_TS_FORWARD = 3 -- move txn timestamp forward. eg. read/write conflict

-- cdefs
ffi.cdef [[
typedef struct luact_dht_range {
	luact_dht_key_t start_key;
	luact_dht_key_t end_key;
	uint8_t n_replica, kind, replica_available, padd;
	luact_mvcc_stats_t stats;
	luact_uuid_t replicas[0];		//arbiter actors' uuid
} luact_dht_range_t;
typedef struct luact_dht_kind {
	char prefix[2];
	uint8_t txnl, padd;
} luact_dht_kind_t;
]]


-- kind data
--[[
### [min]    : unused

### [\0, \1) : un-splittable system keys (configuration) => つまり全てroot rangeのノードに保持される
```
[\0\0, \0\1) : kind (独立したdhtを管理する単位)の名前   \0\0 + numerical id => kind name これによってnumerical id (within 1byte)が予約される
[\0\1, \1  ) : reserved
```

### [\1, \40) : splittable system keys => 数の多いconfiguration, あるいはtxnのいらないユーザーデータ 
```
[\1, \2 ) : txn (分散トランザクションの管理情報) のmeta2 ranges \1\0 + key, non-transactional
[\2, \3 ) : vid (仮想アクターのidと物理アドレスのマッピング) のmeta2 ranges \1\1 + key, non-transactional
[\3, \4 ) : vid state (仮想アクターの永続化される状態,{vid+key}と{value}のマッピング) のmeta2 ranges \1\2 + key, transactional
[\4, \40) : reserved
```

### [\40,\ff) : splittable user keys (max 180)
```
[\40,\41) : user data : category 1のmeta2 ranges \40 + key
[\41,\42) : user data : category 2のmeta2 ranges \41 + key
...
[\fe,\ff) : user data : category 180のmeta2 ranges \fe + key
```

### [\3,max] : reserved
]]
local function new_kind(prefix, txnl)
	local p = memory.alloc_typed('luact_dht_kind_t')
	ffi.copy(p.prefix, prefix, #prefix)
	p.txnl = txnl
	return p
end
local kind_map = {
	-- system category (built in)
	[_M.KIND_META1] = new_kind("", true),
	[_M.KIND_META2] = new_kind("", true),
	[_M.KIND_TXN] = new_kind(string.char(1), false),
	[_M.KIND_VID] = new_kind(string.char(2), false),
	[_M.KIND_STATE] = new_kind(string.char(3), true),
}
local n_data_kind = 0
-- txnl : use transactional operation? 
-- *caution* txnl==false does not means split does not need transaction. 
-- range data of txnl == false still need txn to split. 
local function create_kind(id, txnl)
	kind_map[id] = new_kind(string.char(2, id), _M.DATA_FAMILY, txnl)
end


-- common helper
local function make_arbiter_id(kind, k, kl)
	return string.char(kind)..ffi.string(k, kl)
end
local function make_metakey(kind, k, kl)
	if kind > _M.KIND_META2 then
		local kind = kind_map[kind]
		local p = memory.managed_alloc_typed('char', kl + 1)
		ffi.copy(p, kind.prefix, 1)
		ffi.copy(p + 1, k, kl)
		return p, kl + 1
	else
		return k, kl
	end
end
local function make_syskey(category, k, kl)
	return string.char(0, category)..ffi.string(k, kl)
end

-- synchorinized merge
local function sync_merge(storage, stats, k, kl, v, vl, ts, txn)
	return storage:rawmerge(stats, k, kl, v, vl, ts, txn)
end
local function rawget_to_s(v, vl)
	if v then
		local s = ffi.string(v, vl)
		memory.free(v)
		return s
	end
end




-- range
local range_mt = {}
range_mt.__index = range_mt
function range_mt.size(n_replica)
	return ffi.sizeof('luact_dht_range_t') + (n_replica * ffi.sizeof('luact_uuid_t'))
end
function range_mt.n_replica_from_size(size)
	return math.ceil((size - ffi.sizeof('luact_dht_range_t')) / ffi.sizeof('luact_uuid_t'))
end
function range_mt.alloc(n_replica)
	local p = ffi.cast('luact_dht_range_t*', memory.alloc(range_mt.size(n_replica)))
	p.n_replica = n_replica
	p.replica_available = 0
	return p
end
function range_mt:init(start_key, end_key, kind, category)
	self.start_key = start_key
	self.end_key = end_key
	self.kind = kind
	self.replica_available = 0
	self.stats:init()
	uuid.invalidate(self.replicas[0])
end
function range_mt:__len()
	return range_mt.size(self.n_replica)
end
function range_mt:__tostring()
	local s = 'range('..tonumber(self.kind)..')['..self.start_key:as_digest().."]..["..self.end_key:as_digest().."]"
	s = s..' ('..tostring(ffi.cast('void *', self))..')\n'
	if self.replica_available > 0 then
		for i=0, tonumber(self.replica_available)-1 do
			s = s.."replica"..tostring(i).." "..tostring(self.replicas[i]).."\n"
		end
	else
		s = s.."no replica assigned\n"
	end
	return s
end
-- should call after range registered to range manager caches/ranges
function range_mt:start_replica_set(remote)
	remote = remote or actor.root_of(nil, luact.thread_id)
	-- after this arbiter become leader, range_mt:change_replica_set is called, and it starts scanner.
	-- scanner check number of replica and if short, call add_replica_set to add replica.
	local a = remote.arbiter(
		self:arbiter_id(), scanner.range_fsm_factory, { initial_node = true }, self
	)
	-- wait for this node becoming leader
	while true do
		if self.replica_available >= _M.NUM_REPLICA then
			break
		end
		luact.clock.sleep(0.5)
	end
	logger.notice('start_replica_set',self)
end
function range_mt:debug_add_replica(a)
	-- print('debug_add_replica', a, self.replica_available, self)
	self.replicas[self.replica_available] = a
	self.replica_available = self.replica_available + 1
end

function range_mt:clone(gc)
	local p = ffi.cast('luact_dht_range_t*', memory.alloc(#self))
	ffi.copy(p, self, #self)
	if gc then
		ffi.gc(p, memory.free)
	end
	return p
end
function range_mt:fin()
	-- TODO : consider when range need to be removed, and do correct finalization
	if self:is_lead_by_this_node() then
		local rft = raft.find(self:arbiter_id())
		if rft then
			rft:destroy()
		end
	end
end
function range_mt:arbiter_id()
	return make_arbiter_id(self.kind, self.end_key:as_slice())
end
function range_mt:metakey()
	return make_metakey(self.kind, self.end_key:as_slice())
end
function range_mt:metakind()
	return self.kind > _M.KIND_META2 and _M.KIND_META2 or _M.KIND_META1
end
function range_mt:is_lead_by_this_node()
	return uuid.owner_of(self.replicas[0])
end
function range_mt:metarange()
	local mk, mkl = self:metakey()
	return range_manager:find(mk, mkl, self:metakind())
end
function range_mt:cachekey()
	return self.end_key:as_slice()
end
function range_mt:check_replica()
	if self.replica_available < _M.NUM_REPLICA then
		exception.raise('invalid', 'dht', 'not enough replica', self.replica_available, self.n_replica)
	end
end
function range_mt:compute_stats()
	local p = self:partition()
	p:compute_stats(self.stats, 
		self.start_key.p, self.start_key:length(), self.end_key.p, self.end_key:length(), 
		range_manager.clock:issue())
end
function range_mt:is_root_range()
	return self.kind == _M.KIND_META1
end
function range_mt:is_mvcc()
	return kind_map[self.kind].txnl ~= 0
end
-- true if start_key <= k, kl < end_key, false otherwise
function range_mt:include(k, kl)
	return self.start_key:less_than(k, kl) and (not self.end_key:less_than(k, kl))
end
function range_mt:belongs_to(node)
	if self.replica_available <= 0 then
		return false
	end
	local tid, mid
	if node then
		tid, mid = node.thread_id, node.machine_id
	else -- this node
		tid, mid = luact.thread_id, luact.machine_id
	end
	local r = self.replicas[0]
	return tid == uuid.thread_id(r) and mid == uuid.machine_id(r)
end
function range_mt:find_replica_by(node, check_thread_id)
	for i=0, tonumber(self.replica_available)-1 do
		local r = self.replicas[i]
		if node.machine_id == uuid.machine_id(r) then
			if (not check_thread_id) or (node.thread_id == uuid.thread_id(r)) then
				return r
			end
		end
	end
end
function range_mt:recover_write_error(command, txn, err)
	-- Move txn timestamp forward to response timestamp if applicable.
	if txn.timestamp < command.timestamp then
		logger.info('recover_write_error ts fwd', txn.timestamp, command.timestamp, command)
		txn.timestamp = command.timestamp
	end

	-- Take action on various errors.
	if err:is('txn_ts_uncertainty') then
		-- TODO: Mark the host as certain. certain node skips timestamp check

		-- If the reader encountered a newer write within the uncertainty
		-- interval, move the timestamp forward, just past that write or
		-- up to MaxTimestamp, whichever comes first.
		local exist_ts, max_ts, new_ts = err.args[2], txn:max_timestamp()
		if exist_ts < max_ts then
			new_ts = max_ts
			-- this will change txn.max_ts, but txn.max_ts resets after txn:restart is called
			new_ts:least_greater_of(exist_ts) 
		else
			new_ts = max_ts
		end
		-- Only change the timestamp if we're moving it forward.
		if txn.timestamp < new_ts then
			txn.timestamp = new_ts
		end
		txn:restart(txn.priority, txn.timestamp)
	elseif err:is('txn_aborted') then
		-- Increase timestamp if applicable.
		local target_txn = err.args[1]
		if txn.timestamp < target_txn.timestamp then
			txn.timestamp = target_txn.timestamp
		end
		txn.priority = target_txn.priority
	elseif err:is('txn_push_fail') then
		local resolved_txn = err.args[2]
		if txn.timestamp < resolved_txn.timestamp then
			txn.timestamp:least_greater_of(resolved_txn.timestamp)
		end
		txn:restart(math.max(0, resolved_txn.priority - 1), txn.timestamp)
	elseif err:is('txn_need_retry') then
		local exist_txn = err.args[1]
		if txn.timestamp < exist_txn.timestamp then
			-- logger.info('txn_need_retry: fix ts:', exist_txn, txn)
			txn.timestamp = exist_txn.timestamp
		end
		txn:restart(exist_txn.priority, txn.timestamp)
	end
end
function range_mt:on_cmd_finished(command, txn, cmd_start_msec, ok, r, ...)
	-- if you change txn in this function, the change will propagate to caller. 
	-- note that command:get_txn() is copied txn object, so any change will not affect caller's txn object. 
	-- logger.info('on_cmd_finished', command, txn, ok, r)
	if not ok then
		local err = r
		if err:is('actor_no_body') then
			-- if this node is not owner of this range, invalidate and retrieve range again.
			-- if it is, should be replication delay, so just wait and retry.
			if not range_manager:is_owner(self) then
				range_manager:clear_cache(self)
				local ok, ret = pcall(range_manager.find, range_manager, command:key(), command:keylen(), self.kind)
				if ok then
					ffi.copy(self, ret, ffi.sizeof(self))
					memory.free(ret)
				else
					error(ret)
				end
			end
		elseif txn then
			self:recover_write_error(command, txn, err)
			if err:is('txn_aborted') then
				-- abort this txn by resolving version with aborting status.
				txncoord.resolve_version(txn)
			end
		elseif err:is('txn_required') then -- without txn, thenis should be checked.
			exception.raise('fatal', "TODO : start txn on the fly.")
		end
		error(err)
	elseif txn then
		local rtxn = r
		-- if this command modify database, it should added to transaction.
		if command:create_versioned_value() then
			txncoord.add_cmd(txn, command)
		end
		if command:end_txn() then
			rtxn = ...
			-- If the -linearizable flag is set, we want to make sure that
			-- all the clocks in the system are past the commit timestamp
			-- of the transaction. This is guaranteed if either
			-- - the commit timestamp is MaxOffset behind startNS
			-- - MaxOffset ns were spent in this function
			-- when returning to the client. Below we choose the option
			-- that involves less waiting, which is likely the first one
			-- unless a transaction commits with an odd timestamp.

			-- here, r should be transaction
			local wt = math.min(rtxn.timestamp:walltime(), cmd_start_msec)
			local sleep_ms = txncoord.max_clock_skew() - (util.msec_walltime() - wt)
			if txncoord.opts.linearizable and sleep_ms > 0 then
				log.info(rtxn, ("waiting %d msec on EndTransaction for linearizability"):format(sleep_ms))
				clock.sleep(sleep_ms / 1000)
			end
			if rtxn.status ~= txncoord.STATUS_PENDING then
				-- logger.warn('end txn done: resolve version', r.status)
				txncoord.resolve_version(rtxn)
			end
		end
		-- update with returned transaction (its timestamp may change with retry)
		txn:update_with(rtxn, true)
	end
	-- return normal retval
	return ...
end
function range_mt:txn_retryer(command, txn, fn, ...)
	-- If this call is part of a transaction...
	if txn then
		if txncoord.start_txn(txn) then
			command:get_txn():update_with(txn)
		end
		-- Set the timestamp to the original timestamp for read-only
		-- commands and to the transaction timestamp for read/write
		-- commands.
		if not command:create_versioned_value() then
			command.timestamp = txn.start_at
		else
			command.timestamp = txn.timestamp
		end
	end
	local success, ret = util.retry(txncoord.retry_opts, function (c, f, replica, ...)
		f.id = replica -- because f is callable table object and value "id" will reset once called, we set id on every call
		local r = {pcall(f, replica, ...)}
		if r[1] then
			return util.retry_pattern.STOP, r
		elseif type(r[2]) ~= 'table' then
			exception.raise('runtime', 'txn_retryer', r[2])
		elseif r[2]:is('txn_exists') then
			for idx=0,(#r[2].args)-1,2 do
				local conflict_key = r[2].args[idx+1]
				local conflict_txn = r[2].args[idx+2]
				-- logger.info('conflict_txn', conflict_key, conflict_txn)
				local how = c:create_versioned_value() and _M.RESOLVE_TXN_ABORT or _M.RESOLVE_TXN_TS_FORWARD
				local ok, res_txn = pcall(range_manager.resolve_txn, range_manager, c:get_txn(), conflict_txn, how, c.timestamp)
				if ok then
					-- logger.info('success to resolve: then resolve version', res_txn)
					range_manager:resolve(conflict_key, #conflict_key, c.kind, nil, 0, 0, res_txn.timestamp, res_txn) 
					-- logger.info('success to resolve: then resolve version end')
					return util.retry_pattern.RESTART
				elseif res_txn:is('txn_push_fail') then
					error(res_txn) -- abort writer side retry (will restart entire transaction)
				else
					logger.report('resolve_txn error', res_txn)
					c:get_txn().timestamp:least_greater_of(conflict_txn.timestamp)
					return util.retry_pattern.CONTINUE
				end
			end
		elseif r[2]:is('txn_write_too_old') then
			local exist_ts = r[2].args[2]
			-- just move timestamp forward
			c.timestamp:least_greater_of(exist_ts)
			return util.retry_pattern.RESTART
		else
			-- does some stuff to recover from write error, including tweaking txn
			error(r[2])
		end
	end, command, fn, ...)
	if success then
		return unpack(ret)
	else
		return false, ret
	end
end
function range_mt:write(command, txn, timeout, dictatorial)
	if not dictatorial then
		self:check_replica()
	end
	local replica = self.replicas[0]
	return self:on_cmd_finished(command, txn, util.msec_walltime(), self:txn_retryer(command, txn, replica.write, replica, {command}, timeout, dictatorial))
end
function range_mt:read(k, kl, ts, txn, consistent, timeout)
	local command = cmd.get(self.kind, k, kl, ts, txn, consistent)
	if consistent then
		return self:write(command, txn, timeout)
	else
		self:check_replica()
		local replica = self.replicas[0]
		return self:on_cmd_finished(command, txn, self:txn_retryer(command, txn, replica.read, replica, timeout, command))
	end
end
-- operation to range
function range_mt:get(k, txn, consistent, timeout)
	return self:rawget(k, #k, txn, consistent, timeout)
end
function range_mt:rawget(k, kl, txn, consistent, timeout)
	return self:read(k, kl, range_manager.clock:issue(), txn, consistent, timeout)
end
function range_mt:put(k, v, txn, timeout)
	return self:rawput(k, #k, v, #v, txn, timeout)
end
function range_mt:rawput(k, kl, v, vl, txn, timeout, dictatorial)
	return self:write(cmd.put(self.kind, k, kl, v, vl, range_manager.clock:issue(), txn), txn, timeout, dictatorial)
end
function range_mt:delete(k, txn, timeout)
	return self:rawdelete(k, #k, txn, timeout)
end
function range_mt:rawdelete(k, kl, txn, timeout)
	return self:write(cmd.delete(self.kind, k, kl, range_manager.clock:issue(), txn), txn, timeout)
end
function range_mt:merge(k, v, op, txn, timeout)
	return self:rawmerge(k, #k, v, #v, op, #op, txn, timeout)
end
function range_mt:rawmerge(k, kl, v, vl, op, ol, txn, timeout)
	return self:write(cmd.merge(self.kind, k, kl, v, vl, op, ol, range_manager.clock:issue(), txn), txn, timeout)
end
function range_mt:cas(k, ov, nv, txn, timeout)
	local oval, ol = ov or nil, ov and #ov or 0
	local nval, nl = nv or nil, nv and #nv or 0
	local cas = range_manager.storage_module.op_cas(ov, nv, nil, ovl, nvl)
	return self:rawcas(k, #k, oval, ol, nval, nl, txn, timeout)
end
function range_mt:rawcas(k, kl, ov, ovl, nv, nvl, txn, timeout)
	return self:write(cmd.cas(self.kind, k, kl, ov, ovl, nv, nvl, range_manager.clock:issue(), txn), txn, timeout)
end
function range_mt:watch(k, kl, watcher, method, timeout)
	return self:write(cmd.watch(self.kind, k, kl, watcher, method, range_manager.clock:issue()), txn, timeout)
end
function range_mt:split(at, txn, timeout)
	local spk, spkl
	if at then
		spk, spkl = at, #at
	else
		local p = self:partition()
		spk, spkl = p:find_split_key(self.stats, 
			self.start_key.p, self.start_key:length(),
			self.end_key.p, self.end_key:length())
	end
	return self:write(cmd.split(self.kind, spk, spkl, range_manager.clock:issue(), txn), txn, timeout)
end
function range_mt:merge_hook()
end
function range_mt:scan(k, kl, ek, ekl, n, txn, consistent, scan_type, timeout)
	return self:write(cmd.scan(self.kind, k, kl, ek, ekl, n, range_manager.clock:issue(), txn, consistent, scan_type), txn, timeout)
end
function range_mt:resolve(txn, n, s, sl, e, el, timeout)
	-- does not wait for reply
	local ts = range_manager.clock:issue()
	if s then
		return self:write(cmd.resolve(self.kind, s, sl, e, el, n, ts, txn), txn, timeout)
	else
		return self:write(cmd.resolve(self.kind, self.start_key.p, self.start_key:length(),
			self.end_key.p, self.end_key:length(), n, ts, txn), txn, timeout)
	end
end
function range_mt:end_txn(txn, commit, hook, timeout)
	return self:write(cmd.end_txn(self.kind, range_manager.clock:issue(), txn, commit, hook), txn, timeout)
end
function range_mt:resolve_txn(txn, conflict_txn, how, cmd_ts, timeout)
	return self:write(cmd.resolve_txn(self.kind, range_manager.clock:issue(), txn, conflict_txn, how, cmd_ts), txn, timeout)
end
function range_mt:heartbeat_txn(txn, timeout)
	return self:write(cmd.heartbeat_txn(self.kind, range_manager.clock:issue(), txn), txn, timeout)
end
-- actual processing on replica node of range
function range_mt:exec_get(storage, k, kl, ts, txn, consistent)
	local v, vl, rts
	if self:is_mvcc() then
		v, vl, rts = storage:rawget(k, kl, ts, txn, consistent)
	else
		v, vl = storage:backend():rawget(k, kl)
	end
	return rawget_to_s(v, vl), rts
end
function range_mt:exec_put(storage, k, kl, v, vl, ts, txn)
	if self:is_mvcc() then
		storage:rawput(self.stats, k, kl, v, vl, ts, txn)
		self:on_write_replica_finished()
	else
		storage:backend():rawput(k, kl, v, vl)
	end
end
function range_mt:exec_delete(storage, k, kl, ts, txn)
	if self:is_mvcc() then
		storage:rawdelete(self.stats, k, kl, ts, txn)
		self:on_write_replica_finished()
	else
		strorage:backend():rawdelete(k, kl)
	end
end
function range_mt:exec_merge(storage, k, kl, v, vl, op, ol, ts, txn)
	if not self:is_mvcc() then exception.raise('invalid', 'operation not allowed for non-mvcc range') end
	local r = {storage:rawmerge(self.stats, k, kl, v, vl, ffi.string(op, ol), ts, txn)}
	self:on_write_replica_finished()
	return unpack(r)
end
local function sync_cas(storage, stats, k, kl, o, ol, n, nl, ts, txn)
	local cas = range_manager.storage_module.op_cas(o, n, ol, nl)
	return storage:rawmerge(stats, k, kl, cas, #cas, 'cas', ts, txn)
end
function range_mt:exec_cas(storage, k, kl, o, ol, n, nl, ts, txn)
	if not self:is_mvcc() then exception.raise('invalid', 'operation not allowed for non-mvcc range') end
	local ok, ov = sync_cas(storage, self.stats, k, kl, o, ol, n, nl, ts, txn)
	self:on_write_replica_finished()
	return ok, ov	
end
local range_scan_filter_count
function range_mt.range_scan_filter(k, kl, v, vl, ts, r)
	table.insert(r, ffi.cast('luact_dht_range_t*', v))
	return #r >= range_scan_filter_count
end
-- make last key of this kind by increment last byte of prefix
local scan_end_key_work = memory.alloc_typed('char', 1)
function range_mt:scan_end_key(k, kl)
	if self.kind == _M.KIND_META1 then
		return _M.META2_SCAN_END_KEYSTR, #(_M.META2_SCAN_END_KEYSTR) 
	else -- range data for actual data (DATA_FAMILY)
		ffi.copy(scan_end_key_work, k, 1)
		scan_end_key_work[0] = scan_end_key_work[0] + 1
		return scan_end_key_work, 1
	end
end
function range_mt:exec_scan(storage, k, kl, ek, ekl, n, ts, txn, consistent, scan_type)
	if not self:is_mvcc() then exception.raise('invalid', 'operation not allowed for non-mvcc range') end
	if ekl <= 0 then
		ek, ekl = self:scan_end_key(k, kl)
	end
	-- logger.warn('k/kl', self.kind, n, ('%q'):format(ffi.string(k, kl)), ('%q'):format(ffi.string(ek, ekl)), scan_type)
	if scan_type == cmd.SCAN_TYPE_NORMAL then
		return storage:scan(k, kl, ek, ekl, n, ts, txn, consistent)		
	elseif scan_type == cmd.SCAN_TYPE_RANGE then
		range_scan_filter_count = n
		return storage:scan(k, kl, ek, ekl, range_mt.range_scan_filter, ts, txn, consistent)
	end
end
function range_mt:exec_resolve(storage, s, sl, e, el, n, ts, txn)
	if not self:is_mvcc() then exception.raise('invalid', 'operation not allowed for non-mvcc range') end
	if el > 0 then
		return storage:resolve_versions_in_range(self.stats, s, sl, e, el, n, ts, txn)
	else
		storage:resolve_version(self.stats, s, sl, ts, txn)
		return 1
	end
end
function range_mt:exec_end_txn(storage, ts, txn, commit)
	if self:is_mvcc() then exception.raise('invalid', 'operation not allowed for mvcc range') end
	local k, kl = txncoord.storage_key(txn)
	local v, vl = storage:backend():rawget(k, kl)
	-- if txn record exists, make it gc-able. otherwise nil
	local exist_txn = (v ~= ffi.NULL and ffi.gc(ffi.cast('luact_dht_txn_t *', v), memory.free) or nil)
	local rtxn = txncoord.end_txn(txn, exist_txn, commit)
	-- Persist the transaction record with updated status (& possibly timestamp).
	-- TODO : update stats
	storage:backend():rawput(k, kl, ffi.cast('char *', rtxn), #rtxn)
	return rtxn
end
function range_mt:exec_heartbeat_txn(storage, ts, txn)
	if self:is_mvcc() then exception.raise('invalid', 'operation not allowed for mvcc range') end
	local txk, txkl = txncoord.storage_key(txn)
	local v, vl = storage:backend():rawget(txk, txkl)
	-- If no existing transaction record was found, initialize
	-- to the transaction in the request.
	local exist_txn = (v ~= nil and ffi.gc(ffi.cast('luact_dht_txn_t*', v), memory.free) or txn)
	if exist_txn.status == txncoord.STATUS_PENDING then
		if txn.last_update < ts then
			txn.last_update = ts
		end
		-- logger.notice('hbtxn:', ffi.cast('luact_uuid_t*', txk), exist_txn)
		-- TODO : update stats
		storage:backend():rawput(txk, txkl, ffi.cast('char *', exist_txn), #exist_txn)
	end
	return exist_txn
end
local expiry_ts_work = memory.alloc_typed('pulpo_hlc_t')
function range_mt:exec_resolve_txn(storage, ts, txn, conflict_txn, how, cmd_ts)
	if self:is_mvcc() then exception.raise('invalid', 'operation not allowed for mvcc range') end
	local txk, txkl = txncoord.storage_key(conflict_txn)
	local v, vl = storage:backend():rawget(txk, txkl)
	local resolved_txn
	--logger.info('restxn', ffi.cast('luact_uuid_t*', txk), v)

	if v ~= nil then
		resolved_txn = ffi.gc(ffi.cast('luact_dht_txn_t*', v), memory.free)
		--logger.info('conf/resl', conflict_txn, resolved_txn)
		if resolved_txn.n_retry < conflict_txn.n_retry then
			resolved_txn.n_retry = conflict_txn.n_retry
		end
		if resolved_txn.timestamp < conflict_txn.timestamp then
			resolved_txn.timestamp = conflict_txn.timestamp
		end
		if resolved_txn.priority < conflict_txn.priority then
			resolved_txn.priority = conflict_txn.priority
		end
	else
		-- Some sanity checks for case where we don't find a transaction record.
		if conflict_txn.last_update:is_zero() then
			exception.raise('txn_invalid', 'no txn persisted, yet intent has heartbeat')
		elseif conflict_txn.status ~= txncoord.STATUS_PENDING then
			exception.raise('txn_invalid', 'no txn persisted, yet intent has status', tostring(txn.status))
		end
		resolved_txn = conflict_txn
	end

	-- If already committed or aborted, return success.
	if resolved_txn.status ~= txncoord.STATUS_PENDING then
		return resolved_txn
	end

	-- If we're trying to move the timestamp forward, and it's already
	-- far enough forward, return success.
	if (how == _M.RESOLVE_TXN_TS_FORWARD) and (ts < resolved_txn.timestamp) then
		return resolved_txn
	end

	-- pusherWins bool is true in the event the pusher prevails.
	local pusher_wins

	-- If there's no incoming transaction, the pusher is
	-- non-transactional. We make a random priority, biased by
	-- specified args.Header().UserPriority in this case.
	-- TODO : make_priority cannot reproduce on all replica, fix that
	local priority = txn and txn.priority or txncoord.make_priority(10)

	-- Check for txn timeout.
	if resolved_txn.last_update:is_zero() then
		resolved_txn.last_update = resolved_txn.timestamp
	end
	-- Compute heartbeat expiration.
	local now = ts
	if ts < cmd_ts then
		now = cmd_ts -- if cmd timestamp is later, use it.
	end
	resolved_txn.last_update:copy_to(expiry_ts_work)
	expiry_ts_work:add_walltime(2 * txncoord.opts.txn_heartbeat_interval)
	if expiry_ts_work < now then
		logger.info("pushing expired txn", resolved_txn, expiry_ts_work)
		pusher_wins = true
	elseif txn.n_retry < resolved_txn.n_retry then
		-- Check for an intent from a prior epoch.
		logger.info("pushing intent from previous epoch for txn", resolved_txn)
		pusher_wins = true
	elseif resolved_txn.isolation == txncoord.SNAPSHOT and (how == _M.RESOLVE_TXN_TS_FORWARD) then
		-- logger.info("pushing timestamp for snapshot isolation txn")
		pusher_wins = true
	elseif how == _M.RESOLVE_TXN_CLEANUP then
		-- If just attempting to cleanup old or already-committed txns, don't push.
		pusher_wins = false
	elseif (resolved_txn.priority < priority) or (resolved_txn.priority == priority and txn.timestamp < resolved_txn.timestamp) then
		-- Finally, choose based on priority; if priorities are equal, order by lower txn timestamp.
		-- logger.info("pushing intent from txn with lower priority", resolved_txn, priority)
		pusher_wins = true
	end

	if not pusher_wins then
		-- logger.info("failed to push intent using priority", resolved_txn, txn, priority, resolved_txn.priority, txn.timestamp, resolved_txn.timestamp)
		exception.raise('txn_push_fail', resolved_txn, txn)
	end
	-- Upgrade priority of pushed transaction to one less than pusher's.
	resolved_txn:upgrade_priority(priority - 1)

	if how == _M.RESOLVE_TXN_ABORT then
		-- If aborting transaction, set new status and return success.
		resolved_txn.status = txncoord.STATUS_ABORTED
	elseif how == _M.RESOLVE_TXN_TS_FORWARD then
		-- Otherwise, update timestamp to be one greater than the request's timestamp.
		resolved_txn.timestamp:least_greater_of(ts)
	end
	-- Persist the pushed transaction using zero timestamp for inline value.
	-- TODO : update stats
	storage:backend():rawput(txk, txkl, ffi.cast('char *', resolved_txn), #resolved_txn)
	return resolved_txn
end
function range_mt:exec_watch(storage, k, kl, watcher, method, arg, alen, ts)
	assert(false, "TBD")
end
function range_mt.split_hook(fr, sr)
	-- compute stats after commit done, to minimize write lock duration.
	fr:compute_stats()
	sr:compute_stats()
end
function range_mt:exec_split(storage, at, atl, ts)
	assert(self:is_mvcc(), exception.new('invalid', 'operation not allowed for non-mvcc range'))
	-- create split range data
	txncoord.run_txn({
		on_commit = range_mt.split_hook,
	}, function (txn, fr, sr, mfr, msr)
		-- search for the range which stores split ranges
		fr:update_address(txn, mfr)
		sr:update_address(txn, msr)
	end, rng:make_split_ranges(at, atl))
end
function range_mt:make_split_ranges(a, al)
	if self.start_key:less_than(a, al) and (not self.end_key:less_than_equals(a, al)) then
		local first, second = self:clone(true), self:clone(true)
		first.end_key:init(a, al)
		second.start_key:init(a, al)
		return first, second, first:metarange(), second:metarange()
	else
		exception.raise('invalid', 'split key out of range', ('%q'):format(ffi.string(a, al)), self)
	end
end
function range_mt:update_address(txn, mrng)
	assert(self.kind > _M.KIND_META1)
	mrng = mrng or self:metarange()
	mrng:rawput(mk, mkl, ffi.cast('char *', self), #self, txn)
end
function range_mt:partition()
	return range_manager.partitions[self.kind]
end
function range_mt:start_scan()
	-- become new leader of this range
	scanner.start(self, range_manager)
end
function range_mt:stop_scan()
	scanner.stop(self)
end
function range_mt:on_write_replica_finished()
	-- split if necessary
	local st = self.stats
	if self:is_lead_by_this_node() and 
		((st.bytes_key + st.bytes_val) > range_manager.opts.range_size_max) then
		self:split()
	end
end

-- call from raft module
function range_mt:apply(command)
	assert(self.kind == command.kind)
	range_manager.clock:witness(command.timestamp)
	local r = {command:apply_to(self:partition(), self)}
	return command:get_txn(), unpack(r)
end
function range_mt:fetch_state(read_cmd)
	return self:apply(read_cmd)
end
function range_mt:metadata()
	return {
		key = self.start_key,
	}
end
function range_mt:change_replica_set(type, self_affected, leader, replica_set)
	if not uuid.valid(leader) then
		logger.warn('dht', 'change_replica_set', 'leader_id cleared. wait for next leader_id', self)
		return
	end
	if #replica_set >= self.n_replica then
		for i=1,#replica_set do
			logger.warn('replicas', i, replica_set[i])
		end
		exception.raise('fatal', 'invalid replica set size', #replica_set, self.n_replica)
	end
	local prev_leader = uuid.owner_of(self.replicas[0])
	local current_leader = uuid.owner_of(leader)
	-- change replica set
	self.replicas[0] = leader
	for i=1,#replica_set do
		self.replicas[i] = replica_set[i]
	end
	self.replica_available = (1 + #replica_set)
	-- handle leader change
	if (not prev_leader) and current_leader then
		-- become new leader of this range
		self:start_scan()
		if self:is_root_range() then
			if self ~= range_manager.root_range then
				exception.raise('fatal', 'invalid status', self, range_manager.root_range)
			end
			range_manager:start_root_range_gossiper()
		end
	elseif prev_leader and (not current_leader) then
		-- step down leader of this range
		self:stop_scan()
		if self:is_root_range() then
			if self ~= range_manager.root_range then
				exception.raise('fatal', 'invalid status', self, range_manager.root_range)
			end
			range_manager:stop_root_range_gossiper()
		end
	end
	logger.info('change_replica_set', 'range become', self)
end
function range_mt:snapshot(sr, rb)
	logger.warn('dht', 'range snapshot', 'TBD')
end
function range_mt:restore(sr, rb)
	logger.warn('dht', 'range restore', 'TBD')
end
function range_mt:attach()
	logger.info('range', 'attached raft group', self)
end
function range_mt:detach()
	logger.info('range', 'detached raft group', self)
end
-- sendable ctypes
serde_common.register_ctype('struct', 'luact_dht_range', {
	msgpack = {
		packer = function (pack_procs, buf, ctype_id, obj, length)
			local used = range_mt.size(obj.n_replica)
			local p, ofs = pack_procs.pack_ext_cdata_header(buf, used, ctype_id)
			buf:reserve(used)
			ffi.copy(p + ofs, obj, used)
			return ofs + used
		end,
		unpacker = function (rb, len)
			local n_replica = range_mt.n_replica_from_size(len)
			local ptr = range_mt.alloc(n_replica)
			ffi.copy(ptr, rb:curr_byte_p(), len)
			rb:seek_from_curr(len)
			return ptr
		end,
	},
}, serde_common.LUACT_DHT_RANGE)
serde_common.register_ctype('struct', 'luact_dht_key', nil, serde_common.LUACT_DHT_KEY)
serde_common.register_ctype('union', 'pulpo_hlc', nil, serde_common.LUACT_HLC)
ffi.metatype('luact_dht_range_t', range_mt)




-- range manager
local range_manager_mt = {}
range_manager_mt.__index = range_manager_mt
-- initialize range data structure, including first range of meta1/meta2/vid
-- only can be called by primary node/primary thread. other threads are just join gossip group and 
-- wait for initialization
function range_manager_mt:bootstrap(nodelist)
	local opts = self.opts
	-- create storage for initial dht setting with bootstrap mode
	local root_cf = self:new_partition(_M.KIND_META1, _M.META1_FAMILY_NAME)
	local meta2_cf = self:new_partition(_M.KIND_META2, _M.META2_FAMILY_NAME)
	self:new_partition(_M.KIND_TXN, _M.DATA_FAMILY_NAME)
	self:new_partition(_M.KIND_VID, _M.DATA_FAMILY_NAME)
	self:new_partition(_M.KIND_STATE, _M.DATA_FAMILY_NAME)
	-- start dht gossiper
	self.gossiper = luact.root_actor.gossiper(opts.gossip_port, {
		nodelist = nodelist,
		delegate = function ()
			return (require 'luact.cluster.dht.range').get_manager()
		end
	})
	-- primary thread create initial cluster data structure
	if not nodelist then
		-- create root range
		self.root_range = self:new_range(key.MIN, key.MAX, _M.KIND_META1, true)
		self.root_range:start_replica_set()
		-- put initial meta2 storage into root_range (with writing raft log)
		local meta2 = self:new_range(key.MIN, _M.META2_MAX_KEY, _M.KIND_META2)
		local meta2_key, meta2_key_len = meta2:metakey()
		self.root_range:rawput(meta2_key, meta2_key_len, ffi.cast('char *', meta2), #meta2, nil, nil, true)
		-- put initial default storage into meta2_range (with writing raft log)
		for _, kind in ipairs({_M.KIND_TXN, _M.KIND_VID, _M.KIND_STATE}) do
			local rng = self:new_range(key.MIN, key.MAX, kind)
			local mk, mkl = rng:metakey()
			meta2:rawput(mk, mkl, ffi.cast('char *', rng), #rng, nil, nil, true)
		end
	end
end
-- shutdown range manager.
function range_manager_mt:shutdown(truncate)
	for _, kind in ipairs({_M.KIND_TXN, _M.KIND_VID, _M.KIND_STATE, _M.KIND_META2, _M.KIND_META1}) do
		self:shutdown_kind(kind, truncate)
	end
	self.root_range = false
	self.storage:fin()
end
-- create new kind of dht which name is *name*, 
-- it is caller's responsibility to give unique value for *kind*.
-- otherwise it fails.
function range_manager_mt:bootstrap_kind(name, kind)
	if kind then
		if kind >= _M.BUILTIN_KIND_START and kind <= _M.BUILTIN_KIND_END then
			return kind
		elseif kind < _M.USER_KIND_PREFIX_MIN or kind > _M.USER_KIND_PREFIX_MAX then
			exception.raise('invalid', 'kind value should be within', _M.USER_KIND_PREFIX_MIN, _M.USER_KIND_PREFIX_MAX)
		end
	end
	for k=kind or _M.USER_KIND_PREFIX_MIN, _M.USER_KIND_PREFIX_MAX do
		local syskey = make_syskey(_M.SYSKEY_CATEGORY_KIND, tostring(k))
		local ok, prev = self.root_range:cas(syskey, nil, name) 
		if ok then
			self:new_partition(k, _M.DATA_FAMILY_NAME)
			self:new_range(key.MIN, key.MAX, k)
			return k
		elseif prev == name then
			return k
		end
	end
	exception.raise('invalid', 'there is no unused kind value')
end
-- shutdown specified kind of dht
-- TODO : do following steps to execute cluster-wide shutdown
-- 1. write something to root_range indicate this kind of dht is shutdown
-- 2. leader of raft cluster of root_range send special gossip broadcast to notify shutdown
-- 3. if node receive shutdown gossip message and have any replica of range which have data of this kind of dht, 
--     remove them.
function range_manager_mt:shutdown_kind(kind, truncate)
	if self.caches[kind] then
		self.caches[kind]:clear()
	end
	if self.ranges[kind] then
		self.ranges[kind]:clear(function (rng)
			self:destory_range(rng)
		end)
	end
	self:destroy_partition(kind, truncate)
end
-- create new range
function range_manager_mt:new_range(start_key, end_key, kind, dont_start_replica)
	local r = range_mt.alloc(_M.NUM_REPLICA)
	r:init(start_key, end_key, kind)
	self.ranges[kind]:add(r)
	if kind > _M.KIND_META1 then
		self.caches[kind]:add(r)
	end
	if not dont_start_replica then
		r:start_replica_set()
	end
	return r
end
function range_manager_mt:is_owner(rng)
	return self.ranges[rng.kind]:find(rng:cachekey())
end
function range_manager_mt:create_fsm_for_arbiter(rng)
	local kind = rng.kind
	if not self.ranges[kind] then
		exception.raise('fatal', 'range manager not initialized', kind)
	end
	local r = self.ranges[kind]:find(rng:cachekey())
	if not r then
		r = rng:clone() -- rng is from packet, so volatile
		self.ranges[kind]:add(r)
		if kind > _M.KIND_META1 then
			self.caches[kind]:add(r)
		end
	end
	return r
end
function range_manager_mt:destory_range(rng)
	self:clear_cache(rng)
	self.ranges[rng.kind]:remove(rng)
	rng:fin()
	memory.free(rng)
end
-- if rng is hosted by this node, 
function range_manager_mt:clear_cache(rng)
	if rng.kind > _M.KIND_META1 then
		self.caches[rng.kind]:remove(rng)
	end
end
function range_manager_mt:new_partition(kind, name)
	if #self.partitions >= 255 then
		exception.raise('invalid', 'cannot create new family: full')
	end
	local p = self.partitions.lookup[name]
	if not p then
		p = self.storage:column_family(name)
		self.partitions.lookup[name] = c
	end
	if not self.partitions[kind] then
		self.partitions[kind] = p
		self.ranges[kind] = cache.new(kind)
		if kind > _M.KIND_META1 then
			self.caches[kind] = cache.new(kind)
		end
	end
	return p
end
function range_manager_mt:destroy_partition(kind, truncate)
	local p = self.partitions[kind]
	if not p then
		self.storage:close_column_family(p, truncate)
	end
end
-- find range which contains key (k, kl)
-- search original kind => KIND_META2 => KIND_META1
function range_manager_mt:find(k, kl, kind)
	local r, mk, mkl
	local prefetch = self.opts.range_prefetch_count
	kind = kind or _M.KIND_STATE
	if kind >= _M.NON_METADATA_KIND_START then
		r = self.caches[kind]:find(k, kl)
		-- logger.info('r = ', r)
		if not r then
			mk, mkl = make_metakey(kind, k, kl)
			-- find range from meta ranges
			r = self:find(mk, mkl, _M.NON_METADATA_KIND_START - 1)
			if r then
				r = r:scan(mk, mkl, "", 0, prefetch, nil, true, cmd.SCAN_TYPE_RANGE)
			end
		end
	elseif kind > _M.ROOT_METADATA_KIND then
		r = self.caches[kind]:find(k, kl)
		if not r then
			-- find range from top level meta ranges
			r = self:find(k, kl, kind - 1)
			if r then
				r = r:scan(k, kl, "", 0, prefetch, nil, true, cmd.SCAN_TYPE_RANGE)
			end
		end
	else -- KIND_META1
		return self.root_range
	end
	if r then
		if type(r) == 'table' then
			-- cache all fetched range
			for i=1,#r do
				self.caches[kind]:add(r[i])
			end
			-- first one is our target.
			r = r[1]
		else 
			-- if not table, it means r is from cache, so no need to re-cache
		end
	else
		exception.raise('not_found', 'cannot find range for', ('%d,%q'):format(kind, ffi.string(k, kl)))
	end
	return r
end
function range_manager_mt:find_on_memory(k, kl, kind)
	if kind > _M.KIND_META1 then
		return self.caches[kind]:find(k, kl)
	else
		return self.root_range
	end
end
function range_manager_mt:heartbeat_txn(txn)
	local k, kl = txncoord.storage_key(txn)
	return self:find(k, kl, _M.KIND_TXN):heartbeat_txn(txn)
end
function range_manager_mt:resolve_txn(txn, conflict_txn, how, cmd_ts)
	local k, kl = txncoord.storage_key(conflict_txn)
	return self:find(k, kl, _M.KIND_TXN):resolve_txn(txn, conflict_txn, how, cmd_ts)
end
function range_manager_mt:resolve(k, kl, kind, ek, ekl, n, ts, txn)
	return self:find(k, kl, kind):resolve(txn, n, k, kl, ek, ekl)
end
function range_manager_mt:end_txn(txn, commit)
	local k, kl = txncoord.storage_key(txn)
	self:find(k, kl, _M.KIND_TXN):end_txn(txn, commit)
end
-- get kind id from kind name
function range_manager_mt:parition_name_by_kind(kind)
	local cf = self.partitions[kind]
	for k,v in pairs(self.partitions.lookup) do
		if v == cf then
			return k
		end
	end
end
-- gossip event processors
function range_manager_mt:initialized()
	if self.boot then 
		return true
	elseif self.root_range and (self.root_range.replica_available >= _M.NUM_REPLICA) then
		self.boot = true
		return true
	end
	return false
end
function range_manager_mt:process_join(node, total_nodes)
end
function range_manager_mt:process_leave(node)
	for i=1,#self.caches do
		local c = self.caches[i]
		-- iterate over the range which leader is this node
		c:each_belongs_to_self(function (r, n)
			-- if left node used as replica set 
			local replica = r:find_replica_by(n, self.opts.allow_same_node)
			if replica then
				-- remove left node from replica set
				r.replicas[0]:remove_replica_set(replica)
				-- broadcast
				self.gossiper:broadcast(cmd.gossip.replica_change(r), cmd.GOSSIP_REPLICA_CHANGE)
			end			
		end, node)
	end
end
function range_manager_mt:process_user_event(subkind, p, len)
	if subkind == cmd.GOSSIP_REPLICA_CHANGE then
		p = ffi.cast('luact_dht_gossip_replica_change_t*', p)
		--for i=0,tonumber(p.n_replica)-1 do
		--	logger.report('luact_dht_gossip_replica_change_t', i, p.replicas[i])
		--end
		-- only when this node own or cache corresponding range, need to follow the change
		local ptr, len = p.key:as_slice()
		local r = self:find_on_memory(ptr, len, p.kind) 
		if r then
			if p.n_replica > r.n_replica then
				exception.raise('fatal', 'invalid replica set size', p.n_replica, r.n_replica)
			end
			ffi.copy(r.replicas, p.replicas, ffi.sizeof('luact_uuid_t') * p.n_replica)
			r.replica_available = p.n_replica
			logger.info('replica change', r)
		end
	elseif subkind == cmd.GOSSIP_RANGE_SPLIT then
		-- only when this node own or cache corresponding range, need to follow the change
		local ptr, len = p.key:as_slice()
		local r = self:find_on_memory(ptr, len, p.kind) 
		if r then
			r:split_at(p.split_at)
		end
	elseif subkind == cmd.GOSSIP_ROOT_RANGE then
		p = ffi.cast('luact_dht_range_t*', p)
		if not self.root_range then
			local r = range_mt.alloc(p.n_replica)
			ffi.copy(r, p, range_mt.size(p.n_replica))
			self.root_range = r
		else
			ffi.copy(self.root_range, p, range_mt.size(p.n_replica))
		end
		logger.notice('received root range', self.root_range)
	else
		logger.report('invalid user command', subkind, ('%q'):format(ffi.string(p, len)))
	end
end
function range_manager_mt:memberlist_event(kind, ...)
	logger.warn('memberlist_event', kind, ...)
	if kind == 'start' then
		logger.info('dht', 'gossip start')
	elseif kind == 'join' then
		tentacle(self.process_join, self, ...)
	elseif kind == 'leave' then
		tentacle(self.process_leave, self, ...)
	elseif kind == 'change' then
	elseif kind == 'user' then
		tentacle(self.process_user_event, self, ...)
	else
		logger.report('dht', 'invalid gossip event', kind, ...)
	end
end
function range_manager_mt:user_state()
	-- TODO : send some information to help to choose replica set
	return nil, 0
end

-- tentacle 
function range_manager_mt:start_root_range_gossiper()
	logger.info('start root range gossip', self.gossiper)
	self.threads.root_range = tentacle(self.run_root_range_gossiper, self, self.opts.root_range_send_interval)
end
function range_manager_mt:stop_root_range_gossiper()
	logger.info('stop root range gossip', self.gossiper)
	tentacle.cancel(self.threads.root_range)
	self.threads.root_range = nil
end
function range_manager_mt:run_root_range_gossiper(intv)
	while true do
		if range_manager:initialized() then
			break
		end
		luact.clock.sleep(1.0)
	end
	while true do 
		self.gossiper:broadcast(cmd.gossip.root_range(range_manager.root_range), cmd.GOSSIP_ROOT_RANGE)
		luact.clock.sleep(intv)
	end
end




-- module functions
function _M.get_manager(nodelist, datadir, opts)
	if not range_manager then -- singleton
		if not (datadir and opts) then
			exception.raise('fatal', 'range: initializing parameter not given')
		end
		_M.NUM_REPLICA = opts.n_replica
		local storage_module = require ('luact.storage.mvcc.'..opts.storage) 
		-- local storage_module = require ('luact.cluster.dht.mvcc.'..opts.storage) 
		fs.mkdir(datadir)
		range_manager = setmetatable({
			storage_module = storage_module,
			storage_path = datadir,
			storage = storage_module.open(datadir),
			clock = lamport.new_hlc(),
			root_range = false,
			gossiper = false, -- initialized in dht.lua
			ranges = {}, -- same data structure as cache
			caches = {},
			threads = {}, 
			partitions = {
				lookup = {}, 
			}, 
			boot = false, 
			opts = opts,
		}, range_manager_mt)
		-- start range manager
		range_manager:bootstrap(nodelist)
		logger.info('bootstrap finish')
	end
	return range_manager
end

function _M.destroy_manager()
	if range_manager then
		range_manager:shutdown()
		range_manager = nil
	end
end

return _M
