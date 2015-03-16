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
		return ffi.string(v, vl)
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
	local a = remote.arbiter(
		self:arbiter_id(), scanner.range_fsm_factory, { initial_node = true }, self
	)
	-- wait for this node becoming leader
	while true do
		if uuid.valid(self.replicas[0]) then
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
	assert(false, "TBD")
end
function range_mt:arbiter_id()
	return make_arbiter_id(self.kind, self.end_key:as_slice())
end
function range_mt:metakey()
	return make_metakey(self.kind, self.end_key:as_slice())
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
	return kind_map[self.kind].txnl
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
function range_mt:write(command, timeout, dictatorial)
	if not dictatorial then
		self:check_replica()
	end
	local replica, r, retry
::RETRY::
	replica = self.replicas[0]
	r = {pcall(replica.write, replica, {command}, timeout, dictatorial)}
	if r[1] then
		return unpack(r, 2)
	elseif (not retry) and r[2]:is('actor_no_body') then
		-- invalidate and retrieve range again
		range_manager:clear_cache(self)
		local p, len = self.start_key:as_slice()
		self = range_manager:find(p, len, self.kind)
		retry = true
		goto RETRY
	else
		error(r[2])
	end
end
function range_mt:write_no_wait(command)
	self.replicas[0]:notify_write({command})
end
function range_mt:read(k, kl, ts, txn, consistent, timeout)
	if consistent then
		return self:write(cmd.get(self.kind, k, kl, ts, txn), timeout)
	else
		self:check_replica()
		return self.replicas[0]:read(timeout, k, kl, ts, txn)
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
	self:write(cmd.put(self.kind, k, kl, v, vl, range_manager.clock:issue(), txn), timeout, dictatorial)
	self:on_writer_finished(txn, k, kl)
end
function range_mt:delete(k, txn, timeout)
	return self:rawdelete(k, #k, txn, timeout)
end
function range_mt:rawdelete(k, kl, txn, timeout)
	self:write(cmd.delete(self.kind, k, kl, range_manager.clock:issue(), txn), timeout)
	self:on_writer_finished(txn, k, kl)
end
function range_mt:merge(k, v, op, txn, timeout)
	return self:rawmerge(k, #k, v, #v, op, #op, txn, timeout)
end
function range_mt:rawmerge(k, kl, v, vl, op, ol, txn, timeout)
	local r = {self:write(cmd.merge(self.kind, k, kl, v, vl, op, ol, range_manager.clock:issue(), txn), timeout)}
	self:on_writer_finished(txn, k, kl)
	return unpack(r)
end
function range_mt:cas(k, ov, nv, txn, timeout)
	local oval, ol = ov or nil, ov and #ov or 0
	local nval, nl = nv or nil, nv and #nv or 0
	local cas = range_manager.storage_module.op_cas(ov, nv, nil, ovl, nvl)
	return self:rawcas(k, #k, oval, ol, nval, nl, txn, timeout)
end
function range_mt:rawcas(k, kl, ov, ovl, nv, nvl, txn, timeout)
	local r = {self:write(cmd.cas(self.kind, k, kl, ov, ovl, nv, nvl, range_manager.clock:issue(), txn), timeout)}
	self:on_writer_finished(txn, k, kl)
	return unpack(r)
end
function range_mt:watch(k, kl, watcher, method, timeout)
	return self:write(cmd.watch(self.kind, k, kl, watcher, method, range_manager.clock:issue()), timeout)
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
	return self:write(cmd.split(self.kind, spk, spkl, range_manager.clock:issue(), txn), timeout)
end
function range_mt:scan(k, kl, n, txn, timeout)
	return self:write(cmd.scan(self.kind, k, kl, n, range_manager.clock:issue(), txn), timeout)
end
function range_mt:resolve(txn, n, s, sl, e, el)
	-- does not wait for reply
	local ts = range_manager.clock:issue()
	if s then
		return self:write_no_wait(cmd.resolve(s, sl, e, el, n, ts, txn))
	else
		return self:write_no_wait(cmd.resolve(self.start_key.p, self.start_key:length(),
			self.end_key.p, self.end_key:length(), n, ts, txn))
	end
end
-- actual processing on replica node of range
function range_mt:exec_get(storage, k, kl, ts, txn)
	if self:is_mvcc() then
		return rawget_to_s(storage:rawget(k, kl, ts, txn))
	else
		return rawget_to_s(storage:backend():rawget(k, kl))
	end
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
function range_mt:exec_merge(storage, k, kl, v, vl, op, ts, txn)
	assert(self:is_mvcc(), exception.new('invalid', 'operation not allowed for non-mvcc range'))
	local r = {storage:rawmerge(self.stats, k, kl, v, vl, op, ts, txn)}
	self:on_write_replica_finished()
	return unpack(r)
end
local function sync_cas(storage, stats, k, kl, o, ol, n, nl, ts, txn)
	local cas = range_manager.storage_module.op_cas(o, n, ol, nl)
	return storage:rawmerge(stats, k, kl, cas, #cas, 'cas', ts, txn)
end
function range_mt:exec_cas(storage, k, kl, o, ol, n, nl, ts, txn)
	assert(self:is_mvcc(), exception.new('invalid', 'operation not allowed for non-mvcc range'))
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
	if self.kind >= _M.NON_METADATA_KIND_START then
		exception.raise('invalid', 'this range, should not scanned', self)
	elseif self.kind == _M.KIND_META1 then
		return _M.META2_SCAN_END_KEYSTR, #(_M.META2_SCAN_END_KEYSTR) 
	else -- range data for actual data (DATA_FAMILY)
		ffi.copy(scan_end_key_work, k, 1)
		scan_end_key_work[0] = scan_end_key_work[0] + 1
		return scan_end_key_work, 1
	end
end
function range_mt:exec_scan(storage, k, kl, n, ts, txn)
	assert(self:is_mvcc(), exception.new('invalid', 'operation not allowed for non-mvcc range'))
	local ek, ekl = self:scan_end_key(k, kl)
	logger.warn('k/kl', self.kind, n, ('%q'):format(ffi.string(k, kl)), ('%q'):format(ffi.string(ek, ekl)))
	range_scan_filter_count = n
	return storage:scan(k, kl, ek, ekl, range_mt.range_scan_filter, ts, txn)
end
function range_mt:exec_resolve(storage, s, sl, e, el, n, ts, txn)
	assert(self:is_mvcc(), exception.new('invalid', 'operation not allowed for non-mvcc range'))
	return storage:resolve_versions_in_range(self.stats, s, sl, e, el, n, ts, txn)
end
function range_mt:exec_watch(storage, k, kl, watcher, method, arg, alen, ts)
	assert(false, "TBD")
end
function range_mt:exec_split(storage, at, atl, ts)
	assert(self:is_mvcc(), exception.new('invalid', 'operation not allowed for non-mvcc range'))
	-- create split range data
	local new, updated = rng:make_split_ranges(at, atl)
	range_manager:run_txn(function (txn, new_rng, update_rng)
		-- search for the range which stores split ranges
		new_rng:update_address(txn)
		update_rng:update_address(txn)
	end, new, updated)
end
function range_mt:make_split_ranges(a, al)
	if self.start_key:less_than(a, al) and (not self.end_key:less_than_equals(a, al)) then
		local new = self:clone()
		new.start_key:init(a, al)
		self.end_key:init(a, al)
		new:compute_stats()
		self:compute_stats()
		return new, self
	else
		exception.raise('invalid', 'split key out of range', ('%q'):format(ffi.string(a, al)), self)
	end
end
function range_mt:update_address(txn)
	assert(self.kind > _M.KIND_META1)
	local mk, mkl = self:metakey()
	local rng = range_manager:find(mk, #mk, self.kind - 1)
	rng:rawput(mk, mkl, ffi.cast('char *', self), #self, txn)
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
function range_mt:on_writer_finished(txn, k, kl)
	--if txn then
	--	txn.coord:add_key(k, kl)
	--end
end
function range_mt:on_write_replica_finished()
	-- split if necessary
	local st = self.stats
	if (st.bytes_key + st.bytes_val) > range_manager.opts.range_size_max then
		self:split()
	end
end

-- call from raft module
function range_mt:apply(cmd)
	assert(self.kind == cmd.kind)
	local p = self:partition()
	range_manager.clock:witness(cmd.timestamp)
	return cmd:apply_to(p, self)
end
function range_mt:metadata()
	return {
		key = self.start_key,
	}
end
function range_mt:fetch_state(k, kl, ts, txn)
	local p = self:partition()
	return rawget_to_s(p:rawget(k, kl, ts, txn))
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
			return ffi.gc(ptr, memory.free)
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
function range_manager_mt:shutdown()
	self.caches = {}
	self.ranges = {}
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
	assert(false, "TBD")
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
function range_manager_mt:new_txn(isolation)
	return txncoord.new_txn(isolation)
end
function range_manager_mt:fin_txn(txn, commit)
	return txncoord.fin_txn(txn, commit)
end
function range_manager_mt:run_txn(proc, ...)
	return txncoord.run_txn(self:new_txn(), proc, ...)
end
-- find range which contains key (k, kl)
-- search original kind => KIND_META2 => KIND_META1
function range_manager_mt:find(k, kl, kind)
	local r, mk, mkl
	local prefetch = self.opts.range_prefetch
	kind = kind or _M.KIND_VID
	if kind >= _M.NON_METADATA_KIND_START then
		r = self.caches[kind]:find(k, kl)
		-- logger.info('r = ', r)
		if not r then
			mk, mkl = make_metakey(kind, k, kl)
			-- find range from meta ranges
			r = self:find(mk, mkl, _M.NON_METADATA_KIND_START - 1)
			if r then
				r = r:scan(mk, mkl, prefetch)
			end
		end
	elseif kind > _M.ROOT_METADATA_KIND then
		r = self.caches[kind]:find(k, kl)
		if not r then
			-- find range from top level meta ranges
			r = self:find(k, kl, kind - 1)
			if r then
				r = r:scan(k, kl, prefetch)
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

return _M
