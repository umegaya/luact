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
local _M = require 'luact.cluster.dht.const'
local range_manager

-- 
-- DEFAULT_REPLICA is default number of range replica to maintain
_M.DEFAULT_REPLICA = 3
-- ***_FAMILY_NAME is used for the name of column_family which preserves system kind partition
_M.META1_FAMILY_NAME = '__dht.meta1__'
_M.META2_FAMILY_NAME = '__dht.meta2__'
_M.TXN_FAMILY_NAME = '__dht.txn__'
_M.VID_FAMILY_NAME = '__dht.vid__'
_M.STATE_FAMILY_NAME = '__dht.state__'
-- following declares system range's min and max key
_M.NON_COLOC_KEY_START = memory.alloc_typed('luact_dht_key_t')[0]
key.MIN:next(_M.NON_COLOC_KEY_START)
_M.META1_MIN_KEY = _M.NON_COLOC_KEY_START
_M.META1_MAX_KEY = key.MAX
_M.META2_MIN_KEY = _M.NON_COLOC_KEY_START
_M.META2_MAX_KEY = memory.alloc_typed('luact_dht_key_t')[0]
_M.META2_MAX_KEY:init(string.char(0xFF), 1)
_M.META2_SCAN_END_KEYSTR = string.char(0xFF, 0xFF)
_M.NON_META_MIN_KEY = _M.NON_COLOC_KEY_START
_M.NON_META_MAX_KEY = memory.alloc_typed('luact_dht_key_t')[0]
_M.NON_META_MAX_KEY:init((string.char(0xFF)):rep(key.MAX_LENGTH - 1), key.MAX_LENGTH - 1) -- -1 for 1byte prefix when it convert to meta2 range key
-- resolve txn type
_M.RESOLVE_TXN_CLEANUP = 1 -- cleanup txn. eg. gc?
_M.RESOLVE_TXN_ABORT = 2 -- abort txn. eg. write/write conflict
_M.RESOLVE_TXN_TS_FORWARD = 3 -- move txn timestamp forward. eg. read/write conflict

-- exception
exception.define('range_key')

-- cdef
ffi.cdef [[
typedef struct luact_dht_range {
	luact_dht_key_t start_key;
	luact_dht_key_t end_key;
	uint8_t n_replica, kind, replica_available, size;
	luact_mvcc_stats_t stats;
	luact_uuid_t id;				//range's id
	luact_uuid_t replicas[0];		//range_managers' uuid
} luact_dht_range_t;
]]


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
function range_mt.size(size)
	return ffi.sizeof('luact_dht_range_t') + (size * ffi.sizeof('luact_uuid_t'))
end
function range_mt.size_from_bytes(size)
	return math.ceil((size - ffi.sizeof('luact_dht_range_t')) / ffi.sizeof('luact_uuid_t'))
end
function range_mt.alloc(size, n_replica)
	local p = ffi.cast('luact_dht_range_t*', memory.alloc(range_mt.size(size)))
	p.size = size
	p.n_replica = n_replica or size
	p.replica_available = 0
	return p
end
function range_mt:init(start_key, end_key, kind, replicas, n_replica)
	self.start_key = start_key
	self.end_key = end_key
	self.id = uuid.new()
	self.kind = kind
	self.replica_available = 0
	self.stats:init()
	if not replicas then
		uuid.invalidate(self.replicas[0])
	else
		assert(n_replica <= self.size)
		ffi.copy(self.replicas, replicas, self.n_replica * ffi.sizeof(self.replicas[0]))
	end
end
function range_mt:__len()
	return range_mt.size(self.size)
end
function range_mt:__tostring()
	local s = 'range('..tonumber(self.kind)..'/'..tostring(self.id)..')['..self.start_key:as_digest().."]..["..self.end_key:as_digest().."]"
	s = s..' ('..tostring(ffi.cast('void *', self))..')\n'
	if self.replica_available > 0 then
		s = s .. "replicas"
		for i=0, tonumber(self.replica_available)-1 do
			s = s.." "..tostring(self.replicas[i])
		end
		s = s .."\n"
	else
		s = s.."no replica assigned\n"
	end
	local rft = self:raft_body()
	if rft then
		s = s.."raftgrps "..tostring(rft:leader())
		local rs = rft:replica_set()
		if #rs > 0 then
			for i=1,#rs do
				s = s.." "..tostring(rs[i])
			end
		end
		s = s .."\n"
	else
		s = s.."no raft group\n"
	end
	return s
end
function range_mt:copy_to(rng)
	if #self > #rng then
		local tmp = memory.realloc(rng, #self)
		if not tmp then
			exception.raise('fatal', 'malloc', #self)
		end
		rng = ffi.cast('luact_dht_range_t*', tmp)
	end
	ffi.copy(rng, self, #self)
end
-- initiate initial arbiter and wait until replica set sufficiently formed
local wait_start_replicas_event = {}
function range_mt:start_raft_group(bootstrap)
	local id = self:arbiter_id()
	local rft = self:is_replica_synchorinized_with_raft()
	if not rft then
		local ev = wait_start_replicas_event[id]
		if not ev then
			ev = event.new()
			wait_start_replicas_event[id] = ev
			-- after this arbiter become leader, range_mt:change_replica_set is called, and it starts scanner.
			-- scanner check number of replica and if short, call add_replica_set to add replica.
			luact.root.arbiter(
				self:arbiter_id(), scanner.range_fsm_factory, { 
					initial_node = bootstrap, 
					replica_set = (not bootstrap) and self:computed_raft_replicas(), 
				}, self
			)
		end
		-- wait for this node becoming leader
		-- maintain_replica_set adds new replica and it will call range_mt:change_replica_set, which increases self.replica_available.
		event.join(nil, ev)
		rft = self:raft_body()
	end
	return rft
end
-- compute_raft_replicas computes raft replica set which will be created by these range replicas
-- its just compute replica set, not initialize reft itself.
function range_mt:raft_replica_from_actor_id(id)
	local m, t = uuid.machine_id(id), uuid.thread_id(id)
	return actor.system_process_of(m, t, luact.SYSTEM_PROCESS_RAFT_MANAGER)
end
function range_mt:computed_raft_replicas(rs)
	local r = {}
	for i=0,self.replica_available-1 do
		local rep = self.replicas[i]
		if not uuid.owner_of(rep) then
			table.insert(r, self:raft_replica_from_actor_id(rep))
		end
	end
	return r
end
function range_mt:is_replica_synchorinized_with_raft()
	local rft = self:raft_body()
	if not rft then return nil end
	local leader = rft:leader()
	local replica_set = rft:replica_set()
	-- leader + replica_set should be greater than required replica num 
	-- (luact's raft replica_set does not contains leader)
	return ((uuid.valid(leader) and 1 or 0) + #replica_set) >= self.n_replica and rft or nil 
end
function range_mt:emit_if_synchronized_with_raft()
	local id = self:arbiter_id()
	local ev = wait_start_replicas_event[id]
	if ev then
		if self:is_replica_synchorinized_with_raft() then
			ev:emit('done')
			wait_start_replicas_event[id] = nil
		end
	end
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
	raft.destroy(self:arbiter_id())
end
function range_mt:arbiter_id()
	return key.make_arbiter_id(self.kind, ffi.cast('char *', self.id), ffi.sizeof('luact_uuid_t'))
end
function range_mt:ts_cache()
	return range_manager:ts_cache_by_range(self)
end
function range_mt:metakey()
	return key.make_metakey(self.kind, self:mvcckey())
end
function range_mt:metakind()
	return self.kind > _M.KIND_META2 and _M.KIND_META2 or (self.kind - 1)
end
function range_mt:is_lead_by_this_node()
	return uuid.owner_of(self.replicas[0])
end
function range_mt:metarange(wtxn)
	local mk, mkl = self:metakey()
	return range_manager:find(self:metakind(), mk, mkl, wtxn)
end
function range_mt:cachekey()
	return self.start_key:as_slice()
end
function range_mt:mvcckey()
	return self.end_key:as_slice()
end
function range_mt:check_replica()
	if self.replica_available < _M.NUM_REPLICA then
		exception.raise('invalid', 'dht', 'not enough replica', self.replica_available, self.n_replica)
	end
end
function range_mt:compute_stats()
	local p = self:partition()
	self.stats = p:compute_stats(
		self.start_key.p, self.start_key:length(), 
		self.end_key.p, self.end_key:length(), 
		range_manager.clock:issue())
end
function range_mt:is_root_range()
	return self.kind == _M.KIND_META1
end
function range_mt:is_mvcc()
	return key.is_mvcc(self.kind)
end
-- true if start_key <= k, kl < end_key, false otherwise
function range_mt:contains(k, kl)
	return self.start_key:less_than_equals(k, kl) and (not self.end_key:less_than_equals(k, kl))
end
function range_mt:contains_key_slice_range(k, kl, ek, ekl)
	return self:contains(k, kl) 
end
function range_mt:verify_command_range(command)
	-- logger.warn('txn_retryer', self, command:has_range(), ffi.string(command:key(), command:keylen()))
	if command:has_range() then
		if not self:contains_key_slice_range(command:key(), command:keylen(), command:end_key(), command:end_keylen()) then
			return exception.raise('range_key', ffi.string(command:key(), command:keylen()))
		end
	elseif not self:contains(command:key(), command:keylen()) then
		return exception.raise('range_key', ffi.string(command:key(), command:keylen()))
	end
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
function range_mt:raftlog()
	-- below may cause block due to initialization wait for raft group, 
	-- if still split range creation is ongoing in range_mt:exec_split.
	-- actual initialization is done in range_manager:new_range
	return self:start_raft_group() 
end
function range_mt:raft_body()
	return raft._find_body(self:arbiter_id())
end
function range_mt:write(command, timeout, dictatorial)
	if not uuid.owner_of(self.replicas[0]) then
		return self.replicas[0]:write(command, timeout, dictatorial)
	else
		self:verify_command_range(command)
		local rft = self:raftlog()
		return rft:write({command}, timeout, dictatorial)
	end
end
function range_mt:read(timeout, command)
	if not uuid.owner_of(self.replicas[0]) then
		return self.replicas[0]:read(timeout, command)
	else
		self:verify_command_range(command)
		local rft = self:raftlog()
		return rft:read(timeout, command)
	end
end
-- operation to range
function range_mt:update_address(wtxn)
	assert(self.kind > _M.KIND_META1)
	local mk, mkl = self:metakey()
	range_manager:rawput(self:metakind(), mk, mkl, ffi.cast('char *', self), #self, wtxn)
end
function range_mt:make_split_ranges(a, al)
	if self.start_key:less_than(a, al) and (not self.end_key:less_than_equals(a, al)) then
		local update, new = self:clone(true), self:clone(true)
		update.end_key:init(a, al)
		new.start_key:init(a, al)
		return update, new, update:metarange(), new:metarange()
	else
		exception.raise('invalid', 'split key out of range', ('%q'):format(ffi.string(a, al)), self)
	end
end
function range_mt:split(at)
	local spk, spkl
	if at then
		spk, spkl = at, #at
	else
		local p = self:partition()
		spk, spkl = p:find_split_key(self.stats, 
			self.start_key.p, self.start_key:length(),
			self.end_key.p, self.end_key:length())
	end
	local update, new, update_meta, new_meta = self:make_split_ranges(spk, spkl)
	-- create split range data
	local start = luact.clock.get()
	local ok, r = txncoord.run_txn({
		on_commit = function (txn, ur, nr)
			return cmd.split(ur.kind, ur, nr, range_manager.clock:issue(), txn)
		end,
	}, function (txn, ur, nr)
		-- Create range descriptor for second half of split.
		-- Note that this put must go first in order to locate the
		-- transaction record on the correct range.
		nr:update_address(txn)
		ur:update_address(txn)
	end, update, new)
	return update, new
end

local ADD_REPLICA = 1
local REMOVE_REPLICA = 2
function range_mt:make_ranges_with_new_replica_set(op, replica_set)
	local updated
	if op == ADD_REPLICA then
		updated = range_mt.alloc(self.n_replica + #replica_set)
		updated:init(self.start_key, self.end_key, self.kind, self.replicas, self.n_replica)
		for i=1,#replica_set do
			updated.replicas[self.n_replica + i - 1] = replica_set[i]
		end
	elseif op == REMOVE_REPLICA then
		updated = range_mt.alloc(self.n_replica, 0)
		updated:init(self.start_key, self.end_key, self.kind)
		for i=0, self.replica_available do
			for j=1,#replica_set do
				local found 
				if uuid.equals(self.replica_available[i], replica_set[j]) then
					found = true
					break
				end
			end
			if not found then
				updated.replicas[updated.n_replica] = self.replica_available[i]
				updated.n_replica = updated.n_replica + 1
			end
		end
	end
	return updated
end
function range_mt:add_replica_set(replica_set)
	local update = self:make_ranges_with_new_replica_set(ADD_REPLICA, replica_set)
	local ok, r = txncoord.run_txn({
		on_commit = function (txn, ur, rs)
			return cmd.change_replicas(ur.kind, ur, range_manager.clock:issue(), txn)
		end,
	}, function (txn, ur, rs)
		-- Create range descriptor for second half of split.
		-- Note that this put must go first in order to locate the
		-- transaction record on the correct range.
		ur:update_address(txn)
		local rft = ur:raft_body()
		rft:add_replica_set(rs)
	end, update, replica_set)
	return update
end
function range_mt:remove_replica_set(replica_set)
	local update = self:make_ranges_with_new_replica_set(REMOVE_REPLICA, replica_set) -- false for 'remove' replicas
	local ok, r = txncoord.run_txn({
		on_commit = function (txn, ur, rs)
			return cmd.change_replicas(ur.kind, ur, range_manager.clock:issue(), txn)
		end,
	}, function (txn, ur, rs)
		-- Create range descriptor for second half of split.
		-- Note that this put must go first in order to locate the
		-- transaction record on the correct range.
		ur:update_address(txn)
		local rft = ur:raft_body()
		rft:remove_replica_set(rs)
	end, update, replica_set)
	return update
end
function range_mt:move_to(machine_id, thread_id)
	local from, to = 
		actor.system_process_of(nil, luact.thread_id, luact.SYSTEM_PROCESS_RAFT_MANAGER)
		actor.system_process_of(machine_id, thread_id, luact.SYSTEM_PROCESS_RAFT_MANAGER)
	self:add_replica_set({to})
	self:remove_replica_set({from})
end
-- actual processing on replica node of range
function range_mt:exec_get(storage, k, kl, ts, txn, consistent)
	local v, vl, rts = storage:rawget(k, kl, ts, txn, consistent)
	return rawget_to_s(v, vl), rts
end
function range_mt:exec_put(storage, k, kl, v, vl, ts, txn)
	storage:rawput(self.stats, k, kl, v, vl, ts, txn)
	self:on_write_replica_finished()
end
function range_mt:exec_delete(storage, k, kl, ts, txn)
	storage:rawdelete(self.stats, k, kl, ts, txn)
	self:on_write_replica_finished()
end
function range_mt:exec_merge(storage, k, kl, v, vl, op, ol, ts, txn)
	local r = {storage:rawmerge(self.stats, k, kl, v, vl, ffi.string(op, ol), ts, txn)}
	self:on_write_replica_finished()
	return unpack(r)
end
local function sync_cas(storage, stats, k, kl, o, ol, n, nl, ts, txn)
	local cas = range_manager.storage_module.op_cas(o, n, ol, nl)
	return storage:rawmerge(stats, k, kl, cas, #cas, 'cas', ts, txn)
end
function range_mt:exec_cas(storage, k, kl, o, ol, n, nl, ts, txn)
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
local scan_end_key_work = memory.alloc_typed('uint8_t', 1)
function range_mt:range_scan_end_key(k, kl)
	if self.kind >= _M.NON_METADATA_KIND_START then
		return self.end_key:as_slice()
	elseif self.kind == _M.KIND_META1 then
		return _M.META2_SCAN_END_KEYSTR, #(_M.META2_SCAN_END_KEYSTR) 
	else -- currently KIND_META2 only
		assert(self.kind == _M.KIND_META2)
		ffi.copy(scan_end_key_work, k, 1)
		scan_end_key_work[0] = scan_end_key_work[0] + 1
		return scan_end_key_work, 1
	end
end
function range_mt:exec_scan(storage, k, kl, ek, ekl, n, ts, txn, consistent, scan_type)
	-- logger.warn('k/kl', self.kind, n, ('%q'):format(ffi.string(k, kl)), ek and ('%q'):format(ffi.string(ek, ekl)), scan_type)
	if not ek then
		ek, ekl = self:range_scan_end_key(k, kl)
	end
	if scan_type == cmd.SCAN_TYPE_NORMAL then
		return storage:scan(k, kl, ek, ekl, n, ts, txn, consistent)		
	elseif scan_type == cmd.SCAN_TYPE_RANGE then
		range_scan_filter_count = n
		return storage:scan(k, kl, ek, ekl, range_mt.range_scan_filter, ts, txn, consistent)
	elseif scan_type == cmd.SCAN_TYPE_DELETE then
		return storage:rawdelete_range(self.stats, k, kl, ek, ekl, n, ts, txn)
	end
end
function range_mt:exec_resolve(storage, s, sl, e, el, n, ts, txn)
	if el > 0 then
		return storage:resolve_versions_in_range(self.stats, s, sl, e, el, n, ts, txn)
	else
		storage:resolve_version(self.stats, s, sl, ts, txn)
		return 1
	end
end
function range_mt:exec_end_txn(storage, ts, txn, commit, cmd_on_commit)
	local k, kl = key.make_txn_key(txn)
	local v, vl = storage:backend():rawget(k, kl)
	-- if txn record exists, make it gc-able. otherwise nil
	local exist_txn = (v ~= nil and ffi.gc(ffi.cast('luact_dht_txn_t *', v), memory.free) or nil)
	local rtxn = txncoord.end_txn(txn, exist_txn, commit)
	-- Persist the transaction record with updated status (& possibly timestamp).
	-- TODO : update stats
	storage:backend():rawput(k, kl, ffi.cast('char *', rtxn), #rtxn)
	if cmd_on_commit then
		-- use current txn (already commited) main usage is resolve version of necessary key
		cmd_on_commit:set_txn(rtxn)
		cmd_on_commit:apply_to(self:partition(), self)
	end
	return rtxn
end
function range_mt:exec_heartbeat_txn(storage, ts, txn)
	local txk, txkl = key.make_txn_key(txn)
	local v, vl = storage:backend():rawget(txk, txkl)
	-- If no existing transaction record was found, initialize
	-- to the transaction in the request.
	local exist_txn = (v ~= nil and ffi.gc(ffi.cast('luact_dht_txn_t*', v), memory.free) or txn)
	if exist_txn.status == txncoord.STATUS_PENDING then
		if txn.last_update < ts then
			txn.last_update = ts
		end
		-- logger.notice('hbtxn:', key.inspect(txk, txkl), txn)
		-- TODO : update stats
		storage:backend():rawput(txk, txkl, ffi.cast('char *', exist_txn), #exist_txn)
	end
	return exist_txn
end
local expiry_ts_work = memory.alloc_typed('pulpo_hlc_t')
function range_mt:exec_resolve_txn(storage, ts, txn, conflict_txn, how, cmd_ts)
	local txk, txkl = key.make_txn_key(conflict_txn)
	local v, vl = storage:backend():rawget(txk, txkl)
	local resolved_txn
	-- logger.notice('txk:::', key.inspect(txk, txkl), conflict_txn, txn)

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
		logger.info('conf:', conflict_txn)
		os.exit(-1)
		if not conflict_txn.last_update:is_zero() then
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
	expiry_ts_work:add_walltime(2 * range_manager.opts.txn_heartbeat_interval)
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
--[[
exec_splitはsplitのフローのうち3.を実行する
splitのフローは以下の通り
1. rangeを分割したデータを作る(updated/new)
2. newを保持するデータを新しくmetarangeに作成する。updatedを保持しているmetarangeのデータを更新する。
　　これらはtransactionalに更新する。
3. 同時にこのraft commandに関与したVM全てで他の処理の割り込みがないタイミングでこの２つのrangeを保持するようにマークする。(range_manager.ranges[kind]の更新)
   この場合、range_manager.rangesはkeyからそれを保持するrangeを高速に検索できるようになっている必要がある(rbtree?)
4. gossipでsplitを通知する
]]
function range_mt:exec_split(storage, update_range, new_range, txn, ts)
	local start = luact.clock.get()
	local k, kl = update_range:cachekey()
	local updated = range_manager:find_range(k, kl, update_range.kind)
	updated.end_key:init(update_range.end_key:as_slice())
	updated:compute_stats()
	-- update new range's ts_cache. it should be same as split source's, without initializing raft group
	local created = range_manager:new_range(new_range.start_key, new_range.end_key, new_range.kind, new_range.replicas, new_range.n_replica)
	created:ts_cache():merge(updated:ts_cache(), true)
	range_manager:add_cache_range(created) -- add to cache.
	created:compute_stats()
	-- here, all range related setup is done, then start raft group.
	-- luajit VM is single threaded, so any request to *created* never be received after here.
	created:start_raft_group()
	-- notify split via gossip
	range_manager.gossiper:notify_broadcast(cmd.gossip.range_split(self, self.end_key), cmd.GOSSIP_RANGE_SPLIT)
end
function range_mt:exec_change_replica(storage, update_range, txn, ts)
	local k, kl = update_range:cachekey()
	local prev = range_manager:find_range(k, kl, update_range.kind)
	assert(update_range:raft_body().fsm == prev)
	local updated = update_range:clone()
	range_manager:add_owned_range(updated)
	updated:raft_body().fsm = updated -- replace fsm of raft
	memory.free(prev)
	-- notify replica change via gossip
	range_manager.gossiper:notify_broadcast(cmd.gossip.replica_change(self), cmd.GOSSIP_REPLICA_CHANGE)
end
function range_mt:partition()
	return range_manager.partitions[self.kind]
end
function range_mt:start_scan()
	-- become new leader of this range
	scanner.start(self, range_manager)
	if self:is_root_range() then
		if self ~= range_manager.root_range then
			exception.raise('fatal', 'invalid status', self, range_manager.root_range)
		end
		range_manager:start_root_range_gossiper()
	end
end
function range_mt:stop_scan()
	scanner.stop(self)
	if self:is_root_range() then
		if self ~= range_manager.root_range then
			exception.raise('fatal', 'invalid status', self, range_manager.root_range)
		end
		range_manager:stop_root_range_gossiper()
	end
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
	if not command then
		logger.error('command nil')
	end
	assert(self.kind == command.kind)
	-- if command uses ts cache (and not readonly), forward ts according to range's latest read/write timestamp
	if command:use_ts_cache() and (not command:is_readonly()) then
		local rts, wts = self:ts_cache():latest_ts(command:key(), command:keylen(), command:end_key(), command:end_keylen(), command:get_txn())
		-- Always push the timestamp forward if there's been a read which
		-- occurred after our txn timestamp.
		if rts >= command.timestamp then
			command.timestamp:least_greater_of(rts)
			-- logger.info("ts forward to rts", ('%q'):format(ffi.string(command:key(),command:keylen())), command:end_key_str(), command.timestamp, rts, command)
		end
		-- If there's a newer write timestamp...
		if wts >= command.timestamp then
			-- If we're in a txn, we still go ahead and try the write since
			-- we want to avoid restarting the transaction in the event that
			-- there isn't an intent or the intent can be pushed by us.
			-- 
			-- If we're not in a txn, it's trivial to just advance our timestamp.
			if not command:get_txn() then
				command.timestamp:least_greater_of(wts)
				-- logger.info("ts forward to wts", command.timestamp, wts, command)
			end
		end
	end
	local r = {pcall(command.apply_to, command, self:partition(), self)}
	if r[1] then
		-- if command success, update ts_cache with this timestamp and returns it to caller 
		if command:use_ts_cache() then
			--[[ 
			logger.info('command use ts cache: update cache:', 
				command, ('%q'):format(ffi.string(command:key(), command:keylen())), command:end_key_str(), 
				command.timestamp, command:get_txn(), command:is_readonly())
			--]]
			self:ts_cache():add(command:key(), command:keylen(), command:end_key(), command:end_keylen(), command.timestamp, command:get_txn(), command:is_readonly())
		end
		return true, command:get_txn(), command.timestamp, unpack(r, 2)
	else
		return false, unpack(r, 2), command.timestamp
	end
end
function range_mt:fetch_state(read_cmd)
	return self:apply(read_cmd)
end
function range_mt:metadata()
	return {
		key = self.start_key,
	}
end
-- raft2rm maintains mapping of ip => range manager actor
local machine_and_thread_to_rm = {}
local function manager_actor_from(raft_uuid)
	return actor.system_process_of(
		uuid.machine_id(raft_uuid), uuid.thread_id(raft_uuid), luact.SYSTEM_PROCESS_RANGE_MANAGER)
end
function range_mt:debug_add_replica(a)
	self.replicas[self.replica_available] = manager_actor_from(a)
	self.replica_available = self.replica_available + 1
	self:emit_if_synchronized_with_raft()
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
	self.replicas[0] = manager_actor_from(leader)
	for i=1,#replica_set do
		self.replicas[i] = manager_actor_from(replica_set[i])
	end
	self.replica_available = (1 + #replica_set)
	-- handle leader change
	if (not prev_leader) and current_leader then
		-- become new leader of this range
		self:start_scan()
	elseif prev_leader and (not current_leader) then
		-- step down leader of this range
		self:stop_scan()
	end
	self:emit_if_synchronized_with_raft()
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
			local used = range_mt.size(obj.size)
			local p, ofs = pack_procs.pack_ext_cdata_header(buf, used, ctype_id)
			buf:reserve(used)
			ffi.copy(p + ofs, obj, used)
			return ofs + used
		end,
		unpacker = function (rb, len)
			local size = range_mt.size_from_bytes(len)
			local ptr = range_mt.alloc(size)
			ffi.copy(ptr, rb:curr_byte_p(), len)
			rb:seek_from_curr(len)
			return ptr
		end,
	},
}, serde_common.LUACT_DHT_RANGE)
serde_common.register_ctype('struct', 'luact_dht_key', nil, serde_common.LUACT_DHT_KEY)
serde_common.register_ctype('union', 'pulpo_hlc', nil, serde_common.LUACT_HLC)
ffi.metatype('luact_dht_range_t', range_mt) -- cdef is done in range_cdef.lua




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
	self:new_partition(_M.KIND_VID, _M.VID_FAMILY_NAME)
	self:new_partition(_M.KIND_STATE, _M.STATE_FAMILY_NAME)
	-- start dht gossiper
	self.gossiper = luact.root_actor.gossiper(opts.gossip_port, {
		nodelist = nodelist,
		delegate = function ()
			return (require 'luact.cluster.dht.range').get_manager()
		end
	})
	-- primary thread create initial cluster data structure
	if not nodelist then
		-- create root range. 
		self.root_range = self:new_range(_M.META1_MIN_KEY, _M.META1_MAX_KEY, _M.KIND_META1)
		-- because of check in range:change_replica_set, first we need to initialize root_range before starts_raft_group of it.
		self.root_range:start_raft_group(true)
		-- put initial meta2 storage into root_range (with writing raft log)
		local meta2 = self:new_range(_M.META2_MIN_KEY, _M.META2_MAX_KEY, _M.KIND_META2)
		meta2:start_raft_group(true)
		local meta2_key, meta2_key_len = meta2:metakey()
		self:rawput(_M.KIND_META1, meta2_key, meta2_key_len, ffi.cast('char *', meta2), #meta2, nil, nil, true)
		-- put initial default storage into meta2_range (with writing raft log)
		for kind=_M.NON_METADATA_KIND_START,_M.NON_METADATA_KIND_END do
			local rng = self:new_range(_M.NON_META_MIN_KEY, _M.NON_META_MAX_KEY, kind)
			rng:start_raft_group(true)
			local mk, mkl = rng:metakey()
			self:rawput(_M.KIND_META2, mk, mkl, ffi.cast('char *', rng), #rng, nil, nil, true)
		end
	end
end
-- shutdown range manager.
function range_manager_mt:shutdown(truncate)
	for kind=_M.ROOT_METADATA_KIND,_M.NON_METADATA_KIND_END do
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
		elseif kind < _M.USER_KIND_PREFIX_START or kind > _M.USER_KIND_PREFIX_END then
			exception.raise('invalid', 'kind value should be within', _M.USER_KIND_PREFIX_START, _M.USER_KIND_PREFIX_END)
		end
	end
	for k=kind or _M.USER_KIND_PREFIX_START, _M.USER_KIND_PREFIX_END do
		local syskey = key.make_user_kind_syskey(string.char(k), 1)
		local ok, prev = self.root_range:cas(syskey, nil, name) 
		if ok then
			self:new_partition(k)
			local rng = self:new_range(_M.NON_META_MIN_KEY, _M.NON_META_MAX_KEY, k)
			rng:start_raft_group(true)
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
			self:destroy_range(rng)
		end)
	end
	self:destroy_partition(kind, truncate)
end
-- create new range
function range_manager_mt:new_range(start_key, end_key, kind, replica_set, n_replica)
	local r = range_mt.alloc(n_replica and math.max(n_replica, _M.NUM_REPLICA) or _M.NUM_REPLICA)
	r:init(start_key, end_key, kind, replica_set, n_replica)
	self:add_owned_range(r)
	self:add_cache_range(r)
	return r
end
function range_manager_mt:destroy_range(rng)
	local k = rng:arbiter_id()
	local c = self.ts_caches[k] 
	self.ts_caches[k] = nil
	c:fin()
	self:clear_cache(rng)
	self.ranges[rng.kind]:remove(rng)
	rng:fin()
	memory.free(rng)
end
-- TODO : max clock skew should be change according to cluster environment, so measure it.
function range_manager_mt:max_clock_skew()
	return self.opts.max_clock_skew
end
function range_manager_mt:ts_cache_duration()
	if self.opts.ts_cache_duration then
		return self.opts.ts_cache_duration
	end
	return self.opts.txn_heartbeat_interval * 2
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
		self:new_partition(rng.kind)
		self:add_owned_range(r)
		self:add_cache_range(r)
	end
	return r
end
function range_manager_mt:add_owned_range(rng)
	self.ranges[rng.kind]:add(rng)
	local k = rng:arbiter_id()
	if not self.ts_caches[k] then
		self.ts_caches[k] = cache.new_ts(self)
	end
end
function range_manager_mt:add_cache_range(rng)
	if rng.kind > _M.KIND_META1 then
		self.caches[rng.kind]:add(rng)
	end
end
function range_manager_mt:clear_cache(rng)
	if rng.kind > _M.KIND_META1 then
		self.caches[rng.kind]:remove(rng)
	end
end
function range_manager_mt:ts_cache_by_range(rng)
	return self.ts_caches[rng:arbiter_id()]
end
function range_manager_mt:new_partition(kind)
	if #self.partitions >= 255 then
		exception.raise('invalid', 'cannot create new family: full')
	end
	local p = self.partitions[kind]
	if not p then
		p = self.storage:column_family(tostring(kind))
		self.partitions[kind] = p
		self.ranges[kind] = cache.new(kind)
		if kind > _M.KIND_META1 then
			self.caches[kind] = cache.new_range(kind)
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
-- find range which contains key (k, kl) of kind
-- search original kind => KIND_META2 => KIND_META1
function range_manager_mt:find(kind, k, kl, wtxn)
	local r, mk, mkl
	local prefetch = self.opts.range_prefetch_count
	kind = kind or _M.KIND_STATE
	if kind >= _M.NON_METADATA_KIND_START then
		r = self.caches[kind]:find(k, kl)
		-- logger.info('r = ', r)
		if not r then
			mk, mkl = key.make_metakey(kind, k, kl)
			logger.info('metakey', ('%q'):format(ffi.string(mk, mkl)))
			-- find range from meta ranges
			r = self:sr_scan(_M.NON_METADATA_KIND_START - 1, mk, mkl, nil, 0, prefetch, wtxn, true, cmd.SCAN_TYPE_RANGE)
		end
	elseif kind > _M.ROOT_METADATA_KIND then
		r = self.caches[kind]:find(k, kl)
		if not r then
			r = self:sr_scan(kind - 1, k, kl, nil, 0, prefetch, wtxn, true, cmd.SCAN_TYPE_RANGE)
		end
	else -- KIND_META1
		return self.root_range
	end
	if r then
		if type(r) == 'table' then
			-- cache all fetched range
			for i=1,#r do
				self:add_cache_range(r[i])
			end
			-- first one is our target.
			r = r[1]
		else 
			-- if not table, it means r is from cache, so no need to re-cache
		end
	else
		exception.raise('not_found', 'range', kind, ffi.string(k, kl))
	end
	return r
end
function range_manager_mt:find_range_from_cache(k, kl, kind)
	if kind > _M.KIND_META1 then
		return self.caches[kind]:find(k, kl)
	else
		return self.root_range
	end
end
function range_manager_mt:find_range(k, kl, kind)
	return self.ranges[kind]:find(k, kl)
end

-- update_clock updates system clock (hlc) with considering max_clock_skew.
-- if incoming clock exceeds beyond the margin of max_clock_skew, means too future.
function range_manager_mt:update_clock(ts)
	local max_clock_skew = self:max_clock_skew()
	if max_clock_skew > 0 then
		local skew_wt = util.sec2walltime(max_clock_skew)
		local now = util.msec_walltime()
		local dt = ts:walltime() - now
		if dt > skew_wt then
			exception.raise('txn_future_ts', ts:walltime(), now, dt, skew_mt)
		end
	end
	self.clock:witness(ts)
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
-- process_join handles gossip node join event.
function range_manager_mt:process_join(node, total_nodes)
end
-- process_leave handles gossip node leave event
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
-- process_user_event handles gossip node user event
function range_manager_mt:process_user_event(subkind, p, len)
	if subkind == cmd.GOSSIP_REPLICA_CHANGE then
		p = ffi.cast('luact_dht_gossip_replica_change_t*', p)
		--for i=0,tonumber(p.n_replica)-1 do
		--	logger.report('luact_dht_gossip_replica_change_t', i, p.replicas[i])
		--end
		-- only when this node own or cache corresponding range, need to follow the change
		local ptr, len = p.key:as_slice()
		local r = self:find_range_from_cache(ptr, len, p.kind) 
		if r then
			if p.n_replica > r.size then
				local new = range_mt.alloc(p.n_replica)
				new:init(r.start_key, r.end_key, r.kind, p.replicas, p.n_replica)
				self:add_cache_range(new) -- update cache
				memory.free(r)
			else
				ffi.copy(r.replicas, p.replicas, ffi.sizeof('luact_uuid_t') * p.n_replica)
			end
			r.replica_available = p.n_replica
			logger.info('replica change', r)
		end
	elseif subkind == cmd.GOSSIP_RANGE_SPLIT then
		-- only when this node own or cache corresponding range, need to follow the change
		local ptr, len = p.key:as_slice()
		local r = self:find_range_from_cache(ptr, len, p.kind) 
		if r then
			r:split_at(p.split_at)
		end
	elseif subkind == cmd.GOSSIP_ROOT_RANGE then
		p = ffi.cast('luact_dht_range_t*', p)
		if not self.root_range then
			local r = range_mt.alloc(p.size)
			ffi.copy(r, p, range_mt.size(p.size))
			self.root_range = r
		else
			ffi.copy(self.root_range, p, range_mt.size(p.size))
		end
		logger.notice('received root range', self.root_range)
	else
		logger.report('invalid user command', subkind, ('%q'):format(ffi.string(p, len)))
	end
end
-- memberlist_event handles all gossip node event, by calling sub-callback properly.
function range_manager_mt:memberlist_event(kind, ...)
	--logger.warn('memberlist_event', kind, ...)
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
-- user_state returns gossip user state, which is broadcast with system node information
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



-- common operation helper
-- txn error handling
function range_manager_mt:update_txn_by_response(command, txn, ok, r, r2)
	-- if no transactional, nothing to do.
	if not txn then return end
	-- Move txn timestamp forward to response timestamp. because it will change with timestamp cache.
	if ok and txn.timestamp < r2 then
 		-- logger.info('update_txn_by_response ts fwd', txn.timestamp, r2, command)
 		txn.timestamp = r2
	end
	 -- no error, change is not possible
	if ok then return end 
	local err = r
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
function range_manager_mt:on_cmd_finished(command, wrapped_txn, cmd_start_walltime, ok, r, r2, ...)
	-- if you change txn in this function, the change will propagate to caller. 
	-- note that command:get_txn() is copied txn object, so any change will not affect caller's txn object. 
	-- if not ok then
	-- 	 logger.info('on_cmd_finished', command)--, txn, ok, r, r2, 'callret:', ...)
	-- end
	local txn = self:strip(wrapped_txn)
	self:update_txn_by_response(command, txn, ok, r, r2)
	if not ok then
		local err = r
		if wrapped_txn then
			if err:is('txn_aborted') then
				-- abort this txn by resolving version with aborting status.
				txncoord.resolve_version(txn)
			end
		elseif err:is('txn_required') then -- without txn, thenis should be checked.
			exception.raise('fatal', "TODO : start txn on the fly.")
		end
		error(err)
	elseif wrapped_txn then
		local rtxn = r
		-- if this command modify database, it should added to transaction.
		if command:is_start_txn() then
			txncoord.add_cmd(wrapped_txn, command)
		end
		if command:end_txn() then
			rtxn = ...
			-- If the -linearizable flag is set, we want to make sure that
			-- all the clocks in the system are past the commit timestamp
			-- of the transaction. This is guaranteed if either
			-- * the commit timestamp is MaxOffset behind startNS
			-- * MaxOffset ns were spent in this function
			-- when returning to the client. Below we choose the option
			-- that involves less waiting, which is likely the first one
			-- unless a transaction commits with an odd timestamp.
			
			-- here, r should be transaction
			local wt = math.min(rtxn.timestamp:walltime(), cmd_start_walltime)
			local sleep_ms = util.sec2walltime(self:max_clock_skew()) - (util.msec_walltime() - wt)
			if txncoord.opts.linearizable and sleep_ms > 0 then
				logger.info(rtxn, ("waiting %d msec on EndTransaction for linearizability"):format(sleep_ms))
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
	return ...
end
function range_manager_mt:txn_retryer(command, wrapped_txn, fn, ...)
	-- If this call is part of a transaction...
	if wrapped_txn then
		local txn = wrapped_txn:strip()
		if txncoord.start_txn(txn) then
			command:get_txn():update_with(txn)
		end
		-- Set the timestamp to the original timestamp for read-only
		-- commands and to the transaction timestamp for read/write
		-- commands.
		if command:is_readonly() then
			command.timestamp = txn.start_at
		else
			command.timestamp = txn.timestamp
		end
	end
	-- system clock should be update before tweaking by range's ts cache.
	self:update_clock(command.timestamp)
	-- write raft log with retry
	local success, ret = util.retry(txncoord.retry_opts, function (rm, c, wtxn, f, ...)
		-- r == raft status, command status or raft error, returned transaction or command error, returned timestamp, { return values}
		local r = {pcall(f, ...)}
		if not r[1] then 
			-- if raft error, throw it
			error(r[2]) 
		end
		if r[2] then
			-- if command success, return result values
			return util.retry_pattern.STOP, {unpack(r, 2)}
		end
		-- handling command error
		local err, ret_ts = r[3], r[4]
		if type(err) ~= 'table' then
			-- return timestamp and error
			return util.retry_pattern.STOP, {false, exception.raise('runtime', 'txn_retryer', err), ret_ts}
		elseif err:is('txn_exists') then
			for idx=0,(#err.args)-1,2 do
				local conflict_key = err.args[idx+1]
				local conflict_txn = err.args[idx+2]
				-- logger.info('conflict_txn', conflict_key, conflict_txn)
				local how = c:is_write() and _M.RESOLVE_TXN_ABORT or _M.RESOLVE_TXN_TS_FORWARD
				local ok, res_txn = pcall(rm.resolve_txn, rm, wtxn, conflict_txn, how, c.timestamp)
				if ok then
					-- logger.info('success to resolve: then resolve version', res_txn)
					-- nil, 0 means single key resolve
					rm:resolve(txncoord.wrap(res_txn), 0, conflict_key, #conflict_key, nil, 0, res_txn.timestamp)
				    -- logger.info('success to resolve: then resolve version end')
					return util.retry_pattern.RESTART
				elseif res_txn:is('txn_push_fail') then
					-- return timestamp and error
					return util.retry_pattern.STOP, {false, res_txn, ret_ts}
				else
					logger.report('resolve_txn error', res_txn)
					c:get_txn().timestamp:least_greater_of(conflict_txn.timestamp)
					return util.retry_pattern.CONTINUE
				end
			end
		elseif err:is('txn_write_too_old') then
			local exist_ts = err.args[2]
			-- just move timestamp forward
			c.timestamp:least_greater_of(exist_ts)
			return util.retry_pattern.RESTART
		else
			-- does some stuff to recover from write error, including tweaking txn
			return util.retry_pattern.STOP, {false, err, ret_ts}
		end
	end, self, command, wrapped_txn, fn, ...)
	if success then
		-- because, if success, r[1] is true in above callback, no need to add true for indicating success
		return unpack(ret)
	else
		error(ret) -- some retry error. bug
	end
end
function range_manager_mt:read(timeout, command)
	local rng, refreshed
	local kind, k, kl = command.kind, command:key(), command:keylen()
::again::
	rng = self.ranges[kind]:find_contains(k, kl)
	if not rng then
		rng = self:find(kind, k, kl)
		if not rng then
			exception.raise('not_found', 'range', kind, ffi.string(k, kl))
		end
	end
	local r = {pcall(rng.read, rng, timeout, command)}
	if not r[1] then
		if (not refreshed) and (r[2]:is('range_key')) then
			self:clear_cache(rng)
			refreshed = true
			goto again
		end
		error(r[2])
	else
		return unpack(r, 2)
	end
end
function range_manager_mt:write(command, timeout, dictatorial)
	local rng, refreshed
	local kind, k, kl = command.kind, command:key(), command:keylen()
::again::
	rng = self.ranges[kind]:find_contains(k, kl)
	if not rng then
		rng = self:find(kind, k, kl)
		if not rng then
			exception.raise('not_found', 'range', kind, ffi.string(k, kl))
		end
	end
	local r = {pcall(rng.write, rng, command, timeout, dictatorial)}
	if not r[1] then
		logger.notice('rng error', r[2])
		if (not refreshed) and (r[2]:is('range_key')) then
			self:clear_cache(rng)
			refreshed = true
			goto again
		end
		error(r[2])
	else
		return unpack(r, 2)
	end
end
function range_manager_mt:strip(wtxn, kind, k, kl)
	if wtxn then
		return kind and wtxn:setup_and_strip(kind, k, kl) or wtxn:strip()
	end
end
function range_manager_mt:_write(command, wtxn, timeout, dictatorial)
	return self:on_cmd_finished(command, wtxn, util.msec_walltime(), self:txn_retryer(command, wtxn, self.write, self, command, timeout, dictatorial))
end
function range_manager_mt:_read(kind, k, kl, ts, wtxn, consistent, timeout)
	local command = cmd.get(kind, k, kl, ts, self:strip(wtxn, kind, k, kl), consistent)
	if consistent then
		return self:_write(command, wtxn, timeout)
	else
		return self:on_cmd_finished(command, wtxn, util.msec_walltime(), self:txn_retryer(command, wtxn, self.read, self, timeout, command))
	end
end



-- operation to ranges
--[[
range_manager:[operation](kind, k, kl, args...)
=> find range with kind, k, kl
 => range found!!
  => get raft group of the range
  => calling read/write
 => range not found...
  => find from cache (range_manager:find)
  => messaging to primary range replica of range cache.
   => if range_key error happens, refresh cache and try again. if fail again, there is something fatal.
]]
function range_manager_mt:get(kind, k, wtxn, consistent, timeout)
	return self:rawget(kind, k, #k, wtxn, consistent, timeout)
end
function range_manager_mt:rawget(kind, k, kl, wtxn, consistent, timeout)
	return self:_read(kind, k, kl, range_manager.clock:issue(), wtxn, consistent, timeout)
end
function range_manager_mt:put(kind, k, v, wtxn, timeout)
	return self:rawput(kind, k, #k, v, #v, wtxn, timeout)
end
function range_manager_mt:rawput(kind, k, kl, v, vl, wtxn, timeout, dictatorial)
	return self:_write(cmd.put(kind, k, kl, v, vl, range_manager.clock:issue(), self:strip(wtxn, kind, k, kl)), wtxn, timeout, dictatorial)
end
function range_manager_mt:delete(kind, k, wtxn, timeout)
	return self:rawdelete(kind, k, #k, wtxn, timeout)
end
function range_manager_mt:rawdelete(kind, k, kl, wtxn, timeout)
	return self:_write(cmd.delete(kind, k, kl, range_manager.clock:issue(), self:strip(wtxn, kind, k, kl)), wtxn, timeout)
end
function range_manager_mt:merge(kind, k, v, op, wtxn, timeout)
	return self:rawmerge(kind, k, #k, v, #v, op, #op, wtxn, timeout)
end
function range_manager_mt:rawmerge(kind, k, kl, v, vl, op, ol, wtxn, timeout)
	return self:_write(cmd.merge(kind, k, kl, v, vl, op, ol, range_manager.clock:issue(), self:strip(wtxn, kind, k, kl)), wtxn, timeout)
end
function range_manager_mt:cas(kind, k, ov, nv, wtxn, timeout)
	local oval, ol = ov or nil, ov and #ov or 0
	local nval, nl = nv or nil, nv and #nv or 0
	local cas = range_manager.storage_module.op_cas(ov, nv, nil, ovl, nvl)
	return self:rawcas(kind, k, #k, oval, ol, nval, nl, wtxn, timeout)
end
function range_manager_mt:rawcas(kind, k, kl, ov, ovl, nv, nvl, wtxn, timeout)
	return self:_write(cmd.cas(kind, k, kl, ov, ovl, nv, nvl, range_manager.clock:issue(), self:strip(wtxn, kind, k, kl)), wtxn, timeout)
end
function range_manager_mt:watch(kind, k, kl, watcher, method, wtxn, timeout)
	return self:_write(cmd.watch(kind, k, kl, watcher, method, range_manager.clock:issue()), wtxn, timeout)
end
function range_manager_mt:end_txn(wtxn, commit, hook, timeout)
	local txn = self:strip(wtxn)
	return self:_write(cmd.end_txn(txn.kind, range_manager.clock:issue(), txn, commit, hook), wtxn, timeout)
end
function range_manager_mt:resolve_txn(wtxn, conflict_txn, how, cmd_ts, timeout)
	local txn = self:strip(wtxn)
	return self:_write(cmd.resolve_txn(txn.kind, range_manager.clock:issue(), txn, conflict_txn, how, cmd_ts), wtxn, timeout)
end
function range_manager_mt:heartbeat_txn(wtxn, timeout)
	local txn = self:strip(wtxn)
	return self:_write(cmd.heartbeat_txn(txn.kind, range_manager.clock:issue(), txn), wtxn, timeout)
end

-- span commands 
-- span command has start~end span of key to processing
function range_manager_mt:add_span_command_result(ret, r, limit)
	-- logger.info('add_span_command_result', ret, r, limit)
	assert(not ret or (type(r) == type(ret)))
	if not ret then
		ret = r
	elseif type(r) == 'number' then
		ret = ret + r
	elseif type(r) == 'table' then
		for k,v in pairs(r) do
			ret[k] = v
		end
	elseif type(r) == 'string' then
		ret = ret .. r
	end
	if limit > 0 then
		if type(r) == 'number' then
			limit = limit - r
		elseif type(r) == 'table' then
			for k,v in pairs(r) do
				limit = limit - 1
			end
		elseif type(r) == 'string' then
			limit = limit - #r
		end
		if limit <= 0 then
			limit = false
		end
	end
	return ret, limit
end
function range_manager_mt:invoke_span_command(method, kind, k, kl, ek, ekl, n, ...)
	local rng, remain, ret
	local ck, ckl = k, kl
	while true do
		rng = self:find(kind, ck, ckl)
		remain = ek and rng.end_key:less_than(ek, ekl)
		if remain then
			-- In next iteration, query next range.
			-- It's important that we use the EndKey of the current descriptor
			-- as opposed to the StartKey of the next one: if the former is stale,
			-- it's possible that the next range has since merged the subsequent
			-- one, and unless both descriptors are stale, the next descriptor's
			-- StartKey would move us to the beginning of the current range,
			-- resulting in a duplicate scan.
			local cek, cekl = rng.end_key:as_slice()
			ret, n = self:add_span_command_result(ret, self[method](self, kind, ck, ckl, cek, cekl, n, ...), n)
			if not n then break end
			ck, ckl = rng.end_key:as_slice()
		else
			ret = self:add_span_command_result(ret, self[method](self, kind, ck, ckl, ek, ekl, n, ...), n)
			break
		end
	end
	return ret
end
-- "sr_" functions are for "single range" version of span operation
function range_manager_mt:sr_scan(kind, k, kl, ek, ekl, n, wtxn, consistent, scan_type, timeout)
	return self:_write(cmd.scan(kind, k, kl, ek, ekl, n, range_manager.clock:issue(), self:strip(wtxn, kind, k, kl), consistent, scan_type), wtxn, timeout)
end
function range_manager_mt:sr_delete_range(kind, k, kl, ek, ekl, n, wtxn, timeout)
	return self:sr_scan(kind, k, kl, ek, ekl, n, wtxn, true, cmd.SCAN_TYPE_DELETE, timeout)
end
function range_manager_mt:scan(kind, k, kl, ek, ekl, n, wtxn, consistent, scan_type)
	return self:invoke_span_command("sr_scan", kind, k, kl, ek, ekl, n, wtxn, consistent, scan_type)
end
function range_manager_mt:delete_range(kind, k, kl, ek, ekl, n, wtxn)
	return self:invoke_span_command("sr_delete_range", kind, k, kl, ek, ekl, n, wtxn)
end
-- NOTE : resolve have a span. but no need to care about dividing range request for it
-- because resolve's key range is originally given by finished command, which is already seperated properly.
function range_manager_mt:resolve(wtxn, n, s, sl, e, el, ts, timeout)
	-- does not wait for reply
	ts = range_manager.clock:issue()
	local txn = self:strip(wtxn)
	if s then
		return self:write(cmd.resolve(txn.kind, s, sl, e, el, n, ts, txn), wtxn, timeout)
	else
		s, sl, e, el = self.start_key.p, self.start_key:length(), self.end_key.p, self.end_key:length()
		return self:write(cmd.resolve(txn.kind, s, sl, e, el, n, ts, txn), wtxn, timeout)
	end
end

-- range management (split/merge/rebalance)
-- range_manager only provide manual split. 
function range_manager_mt:split(kind, split_at)
	self:find(kind, split_at, #split_at):split(split_at)
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
			ts_caches = {},
			threads = {}, 
			partitions = {
				lookup = {}, 
			}, 
			boot = false, 
			opts = opts,
		}, range_manager_mt)
		-- start range manager
		range_manager:bootstrap(nodelist)
		assert(_M.manager_actor())
	end
	return range_manager
end

local manager_actor
function _M.manager_actor()
	if not manager_actor then
		manager_actor = luact.supervise(range_manager, { 
			actor = {
				uuid = actor.system_process_of(
					nil, luact.thread_id, luact.SYSTEM_PROCESS_RANGE_MANAGER
				)
			},
		})
	end
	return manager_actor
end

function _M.destroy_manager()
	if range_manager then
		range_manager:shutdown()
		range_manager = nil
	end
	if manager_actor then
		actor.destroy(manager_actor)
		manager_actor = nil
	end
end

function _M.debug_new(n_replica)
	return range_mt.alloc(n_replica)
end

return _M
