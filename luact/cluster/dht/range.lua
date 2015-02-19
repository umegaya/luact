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


-- module share variable
local _M = {}
local range_manager


-- constant
_M.MAX_BYTE = 64 * 1024 * 1024
_M.INITIAL_BYTE = 1 * 1024 * 1024
_M.DEFAULT_REPLICA = 3
_M.META1_FAMILY = '__dht.meta1__'
_M.META2_FAMILY = '__dht.meta2__'
_M.VID_FAMILY = '__dht.vid__'
_M.KIND_META1 = 1
_M.KIND_META2 = 2
_M.KIND_VID = 3

_M.SYSKEY_CATEGORY_KIND = 0

-- cdefs
ffi.cdef [[
typedef struct luact_dht_range {
	luact_dht_key_t start_key;
	luact_dht_key_t end_key;
	uint8_t n_replica, kind, replica_available, padd;
	luact_uuid_t replicas[0];		//arbiter actors' uuid
} luact_dht_range_t;
]]


-- common helper
local function make_metakey(kind, key)
	if kind >= _M.KIND_VID then
		return string.char(kind)..key
	else
		return key
	end
end
-- sys key is stored in root range
local function make_syskey(category, key)
	return '\0'..string.char(category)..key
end
-- synchorinized merge
local function sync_merge(storage, k, kl, v, vl)
	storage:rawmerge(k, kl, v, vl)
	v, vl = storage:rawget(k, kl) -- resolve all logs for this key (thus it processed above merge immediately)
	memory.free(v)
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
function range_mt:init(start_key, end_key, kind)
	self.start_key = start_key
	self.end_key = end_key
	self.kind = kind
	self.replica_available = 0
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
	self.replicas[self.replica_available] = a
	self.replica_available = self.replica_available + 1
end

function range_mt:clone()
	local p = ffi.cast('luact_dht_range_t*', memory.alloc(#self))
	ffi.copy(p, self, #self)
	return p
end
function range_mt:fin()
	-- TODO : consider when range need to be removed, and do correct finalization
	assert(false, "TBD")
end
function range_mt:arbiter_id()
	return self:metakey()
end
function range_mt:metakey()
	return string.char(self.kind)..ffi.string(self.start_key.p, self.start_key.length)
end
function range_mt:check_replica()
	if self.replica_available < _M.NUM_REPLICA then
		exception.raise('invalid', 'dht', 'not enough replica', self.replica_available, self.n_replica)
	end
end
function range_mt:is_root_range()
	return self.kind == _M.KIND_META1
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
-- operation to range
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
		local key = self.start_key
		range_manager:clear_cache(self)
		self = range_manager:find(key.p, key.length, self.kind)
		retry = true
		goto RETRY
	else
		error(r[2])
	end
end
function range_mt:read(k, kl, ts, consistent, timeout)
	if consistent then
		return self:write(cmd.get(self.kind, k, kl, ts), timeout)
	else
		self:check_replica()
		return self.replicas[0]:read(ffi.string(k, kl), timeout)
	end
end
function range_mt:get(k, ts, consistent, timeout)
	return self:rawget(k, #k, ts, consistent, timeout)
end
function range_mt:rawget(k, kl, ts, consistent, timeout)
	return self:read(k, kl, ts, consistent, timeout)
end
function range_mt:put(k, v, ts, timeout)
	return self:rawput(k, #k, v, #v, ts, timeout)
end
function range_mt:rawput(k, kl, v, vl, ts, timeout, dictatorial)
	return self:write(cmd.put(self.kind, k, kl, v, vl, ts), timeout, dictatorial)
end
function range_mt:merge(k, v, ts, timeout)
	return self:rawmerge(k, #k, v, #v, ts, timeout)
end
function range_mt:rawmerge(k, kl, v, vl, ts, timeout)
	return self:write(cmd.merge(self.kind, k, kl, v, vl, ts), timeout)
end
function range_mt:cas(k, ov, nv, ts, timeout)
	local oval, ol = ov or nil, ov and #ov or 0
	local nval, nl = nv or nil, nv and #nv or 0
	local cas = range_manager.storage_module.op_cas(ov, nv, nil, ovl, nvl)
	return self:rawcas(k, #k, oval, ol, nval, nl, ts, timeout)
end
function range_mt:rawcas(k, kl, ov, ovl, nv, nvl, ts, timeout)
	return self:write(cmd.cas(self.kind, k, kl, ov, ovl, nv, nvl, ts), timeout)
end
function range_mt:watch(k, kl, watcher, method, ts, timeout)
	return self:write(cmd.watch(self.kind, k, kl, watcher, method, ts), timeout)
end
function range_mt:split(range_key, ts, timeout)
	return self:write(cmd.split(self.kind, range_key, ts), timeout)
end
function range_mt:scan(k, kl, ts, timeout)
	logger.info('scan', k, kl, ts, timeout)
	local r = self:write(cmd.scan(self.kind, k, kl, ts), timeout)
	logger.info('scan result', r)
	return ffi.cast('luact_dht_range_t *', r)
end
function range_mt:split_at(at)
	assert(false, "TBD")
end
-- actual processing on replica node of range
function range_mt:exec_get(storage, k, kl)
	-- logger.warn('exec_get', storage, tostring(k), kl)
	local v, vl = storage:rawget(k, kl)
	if v ~= ffi.NULL then
		local p = ffi.string(v, vl)
		memory.free(v)
		return p
	end
end
function range_mt:exec_put(storage, k, kl, v, vl, timestamp)
	return storage:rawput(k, kl, v, vl)
end
function range_mt:exec_merge(storage, k, kl, v, vl, timestamp)
	return storage:rawmerge(k, kl, v, vl)
end
range_mt.cas_result = memory.alloc_typed('bool')
local function sync_cas(storage, k, kl, o, ol, n, nl, timestamp)
	range_mt.cas_result[0] = false
	local cas = range_manager.storage_module.op_cas(o, n, range_mt.cas_result, ol, nl)
	sync_merge(storage, k, kl, cas, #cas)
	return range_mt.cas_result[0]
end
function range_mt:exec_cas(storage, k, kl, o, ol, n, nl, timestamp)
	return sync_cas(storage, k, kl, o, ol, n, nl)
end
function range_mt:exec_scan(storage, k, kl, timestamp)
	local it = storage:iterator()
	it:seek(k, kl) --> seek to the smallest of bigger key
	if it:valid() then
		it:prev() --> it seeks to biggest of smallest key
	end
	if not it:valid() then 
		it:first() --> search from first for safety
	end
	logger.info('exec_scan', ('%q'):format(ffi.string(k, kl)), it:valid(), ('%q'):format(ffi.string(it:key())))
	while it:valid() do
		local v, vl = it:val()	
		v = ffi.cast('luact_dht_range_t*', v)
		-- v.start_key <= k, kl <= v.end_key
		if v.start_key:less_than_equals(k, kl) and (not v.end_key:less_than(k, kl)) then
			-- logger.info('exec_scan: returns', v)
			return v
		end
		it:next()
	end
end
function range_mt:exec_watch(storage, k, kl, watcher, method, arg, alen, timestamp)
	assert(false, "TBD")
end
function range_mt:exec_split(storage, k, kl, timestamp)
	assert(false, "TBD")
end
function range_mt:kv_group()
	return range_manager.kv_groups[self.kind]
end
function range_mt:start_scan()
	-- become new leader of this range
	scanner.start(self, range_manager)
end
function range_mt:stop_scan()
	scanner.stop(self)
end

-- call from raft module
function range_mt:apply(cmd)
	assert(self.kind == cmd.kind)
	local cf = self:kv_group()
	range_manager.clock:witness(cmd.timestamp)
	return cmd:apply_to(cf, self)
end
function range_mt:metadata()
	return {
		key = self.start_key,
	}
end
function range_mt:fetch_state(key)
	local cf = self:kv_group()
	return cf:get(key)
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
ffi.metatype('luact_dht_range_t', range_mt)




-- range manager
local range_manager_mt = {}
range_manager_mt.__index = range_manager_mt
-- initialize range data structure, including first range of meta1/meta2/vid
-- only can be called by primary node/primary thread. other threads are just join gossip group and 
-- wait for initialization
function range_manager_mt:bootstrap(nodelist)
	local opts = self.opts
	local cluster_bootstrap = (not nodelist)
	-- create storage for initial dht setting with bootstrap mode
	local root_cf = self:new_kv_group(_M.KIND_META1, _M.META1_FAMILY, cluster_bootstrap)
	local meta2_cf = self:new_kv_group(_M.KIND_META2, _M.META2_FAMILY, cluster_bootstrap)
	local default_cf = self:new_kv_group(_M.KIND_VID, _M.VID_FAMILY, cluster_bootstrap)
	-- start dht gossiper
	self.gossiper = luact.root_actor.gossiper(opts.gossip_port, {
		nodelist = nodelist,
		delegate = function ()
			return (require 'luact.cluster.dht.range').get_manager()
		end
	})
	-- primary thread create initial cluster data structure
	if cluster_bootstrap then
		-- create root range
		self.root_range = self:new_range(key.MIN, key.MAX, _M.KIND_META1, true)
		self.root_range:start_replica_set()
		-- put initial meta2 storage into root_range (with writing raft log)
		local meta2 = self:new_range(key.MIN, key.MAX, _M.KIND_META2)
		local meta2_key = meta2:metakey()
		self.root_range:rawput(
			meta2_key, #meta2_key, ffi.cast('char *', meta2), #meta2, 
			self.clock:issue(), nil, true)
		-- put initial default storage into meta2_range (with writing raft log)
		local default = self:new_range(key.MIN, key.MAX, _M.KIND_VID)
		local default_key = default:metakey()
		meta2:rawput(
			default_key, #default_key, ffi.cast('char *', default), #default, 
			self.clock:issue(), nil, true)
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
function range_manager_mt:bootstrap_kind(kind, name)
	local syskey = make_syskey(_M.SYSKEY_CATEGORY_KIND, tostring(kind))
	if self.root_range:cas(syskey, nil, name) then
		self:new_kv_group(id, name)
		self:new_range(key.MIN, key.MAX, kind)
		return kind
	end
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
	kind = kind or _M.KIND_VID
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
	local r = self.ranges[kind]:find(rng.start_key.p, rng.start_key.length)
	if not r then
		r = range_mt.alloc(rng.n_replica)
		ffi.copy(r, rng, range_mt.size(rng.n_replica))
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
	local kind = rng.kind
	if kind > _M.KIND_META1 then
		self.caches[kind]:remove(rng)
	end
end
function range_manager_mt:new_kv_group(kind, name, cluster_bootstrap)
	if #self.kv_groups >= 255 then
		exception.raise('invalid', 'cannot create new family: full')
	end
	local c = self.kv_groups.lookup[name]
	if not c then
		c = self.storage:column_family(name)
		self.kv_groups[kind] = c
		self.ranges[kind] = cache.new(kind)
		if kind > _M.KIND_META1 then
			self.caches[kind] = cache.new(kind)
		end
		self.kv_groups.lookup[name] = c
	end
	if cluster_bootstrap then
		local syskey = make_syskey(_M.SYSKEY_CATEGORY_KIND, tostring(kind))
		if not sync_cas(c, syskey, #syskey, nil, 0, name, #name) then
			exception.raise('fatal', 'initial kind of dht cannot registered', kind, name)
		end
	end
	return c
end
-- find range which contains key (k, kl)
-- search original kind => KIND_META2 => KIND_META1
--[[
	"\n{key}" (n >= _M.KIND_VID) in ranges/caches KIND_META2 => range which contains addressing info for {key} of range kind n.
	"\n{key}" (n >= _M.KIND_VID) in root_range => range which contains addressing info for "\n{key}" of range meta2
]]
function range_manager_mt:find(k, kl, kind)
	local r
	kind = kind or _M.KIND_VID
	if kind >= _M.KIND_VID then
		r = self.caches[kind]:find(k, kl)
		-- logger.info('r = ', r)
		if not r then
			-- make unique key over all key in kind >= _M.KIND_VID
			k = make_metakey(kind, ffi.string(k, kl))
			kl = #k
			-- find range from meta ranges
			r = self:find(k, kl, _M.KIND_META2)
			if r then
				r = r:scan(k, kl, self.clock:issue())
			end
		end
	elseif kind > _M.KIND_META1 then
		r = self.caches[kind]:find(k, kl)
		if not r then
			-- find range from top level meta ranges
			r = self:find(k, kl, kind - 1)
			if r then
				r = r:scan(k, kl, self.clock:issue())
			end
		end
	else -- KIND_META1
		return self.root_range
	end
	if r then
		self.caches[kind]:add(r)
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
function range_manager_mt:family_name_by_kind(kind)
	local cf = self.kv_groups[kind]
	for k,v in pairs(self.kv_groups.lookup) do
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
		local r = self:find_on_memory(p.key.p, p.key.length, p.kind) 
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
		local r = self:find_on_memory(p.key.p, p.key.length, p.kind) 
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
		local storage_module = require ('luact.storage.'..opts.storage) 
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
			kv_groups = {
				lookup = {}, 
			}, 
			boot = false, 
			opts = opts,
		}, range_manager_mt)
		-- start range manager
		range_manager:bootstrap(nodelist)
	end
	return range_manager
end

return _M
