local ffi = require 'ffiex.init'
local util = require 'pulpo.util'
local memory = require 'pulpo.memory'
local lamport = require 'pulpo.lamport'
local uuid = require 'luact.uuid'
local serde_common = require 'luact.serde.common'

local key = require 'luact.cluster.dht.key'
local txncoord = require 'luact.storage.txncoord'

local _M = {}

ffi.cdef [[
typedef struct luact_dht_cmd_get {
	pulpo_hlc_t timestamp;
	uint8_t kind:8, txn_f:1, consistent:1, padd:6;
	uint16_t kl;
	char p[0];
} luact_dht_cmd_get_t;

typedef struct luact_dht_cmd_put {
	pulpo_hlc_t timestamp;
	uint8_t kind, txn_f;
	uint16_t kl;
	uint32_t vl;
	char p[0];	
} luact_dht_cmd_put_t;

typedef struct luact_dht_cmd_delete {
	pulpo_hlc_t timestamp;
	uint8_t kind, txn_f;
	uint16_t kl;
	char p[0];
} luact_dht_cmd_delete_t;

typedef struct luact_dht_cmd_cas {
	pulpo_hlc_t timestamp;
	uint8_t kind, txn_f;
	uint16_t kl;
	uint32_t ol;
	uint32_t nl;
	char p[0];		
} luact_dht_cmd_cas_t;

typedef struct luact_dht_cmd_merge {
	pulpo_hlc_t timestamp;
	uint8_t kind, txn_f;
	uint16_t kl;
	uint32_t vl;
	uint8_t ol;
	char p[0];		
} luact_dht_cmd_merge_t;

typedef struct luact_dht_cmd_watch {
	pulpo_hlc_t timestamp;
	uint8_t kind, padd;
	uint16_t kl;
	luact_uuid_t watcher;
	uint16_t name_len;
	uint16_t arg_len;
	char p[0];
} luact_dht_cmd_watch_t;

typedef struct luact_dht_cmd_split {
	pulpo_hlc_t timestamp;
	uint8_t kind, txn_f;
	uint16_t kl;
	char p[0];
} luact_dht_cmd_split_t;

typedef struct luact_dht_cmd_scan {
	pulpo_hlc_t timestamp;
	uint8_t kind:8, txn_f:1, consistent:1, padd:6;
	uint16_t kl, n_process;
	char p[0];	
} luact_dht_cmd_scan_t;

typedef struct luact_dht_cmd_resolve {
	pulpo_hlc_t timestamp;
	uint8_t kind, commit;
	uint16_t n_process;
	uint16_t kl, ekl;
	luact_dht_txn_t txn;
	char p[0];
} luact_dht_cmd_resolve_t;

typedef struct luact_dht_cmd_resolve_txn {
	pulpo_hlc_t timestamp;
	pulpo_hlc_t cmd_ts;
	uint8_t kind, how;
	luact_dht_txn_t txn, conflict_txn;
} luact_dht_cmd_resolve_txn_t;

typedef struct luact_dht_cmd_heartbeat_txn {
	pulpo_hlc_t timestamp;
	uint8_t kind, padd;
	luact_dht_txn_t txn;
} luact_dht_cmd_heartbeat_txn_t;

typedef struct luact_dht_cmd_end_txn {
	pulpo_hlc_t timestamp;
	uint8_t kind, commit, padd[2];
	luact_dht_txn_t txn;
} luact_dht_cmd_end_txn_t;

typedef struct luact_dht_gossip_replica_change {
	uint8_t kind, padd[3];
	luact_dht_key_t key;
	uint32_t n_replica;
	luact_uuid_t replicas[0];	
} luact_dht_gossip_replica_change_t;

typedef struct luact_dht_gossip_range_split {
	uint8_t kind, padd[3];
	luact_dht_key_t key;
	luact_dht_key_t split_at;
} luact_dht_gossip_range_split_t;
]]

local function register_ctype(what, name, typename, id)
	local ct = ffi.typeof(what.." "..name .. "*")
	serde_common.register_ctype(what, name, {
		msgpack = {
			packer = function (pack_procs, buf, ctype_id, obj, length)
				local used = #obj
				local p, ofs = pack_procs.pack_ext_cdata_header(buf, used, ctype_id)
				buf:reserve(used)
				ffi.copy(p + ofs, obj, used)
				return ofs + used
			end,
			unpacker = function (rb, len)
				local ptr = ffi.cast(ct, memory.alloc(len))
				ffi.copy(ptr, rb:curr_byte_p(), len)
				rb:seek_from_curr(len)
				return ptr
			end,
		},
	}, id)
end
register_ctype('struct', 'luact_dht_cmd_get', 'luact_dht_cmd_get_t', serde_common.LUACT_DHT_CMD_GET)
register_ctype('struct', 'luact_dht_cmd_put', 'luact_dht_cmd_put_t', serde_common.LUACT_DHT_CMD_PUT)
register_ctype('struct', 'luact_dht_cmd_delete', 'luact_dht_cmd_delete_t', serde_common.LUACT_DHT_CMD_DELETE)
register_ctype('struct', 'luact_dht_cmd_cas', 'luact_dht_cmd_cas_t', serde_common.LUACT_DHT_CMD_CAS)
register_ctype('struct', 'luact_dht_cmd_merge', 'luact_dht_cmd_merge_t', serde_common.LUACT_DHT_CMD_MERGE)
register_ctype('struct', 'luact_dht_cmd_watch', 'luact_dht_cmd_watch_t', serde_common.LUACT_DHT_CMD_WATCH)
register_ctype('struct', 'luact_dht_cmd_split', 'luact_dht_cmd_split_t', serde_common.LUACT_DHT_CMD_SPLIT)
register_ctype('struct', 'luact_dht_cmd_scan', 'luact_dht_cmd_scan_t', serde_common.LUACT_DHT_CMD_SCAN)
register_ctype('struct', 'luact_dht_cmd_resolve', 'luact_dht_cmd_resolve_t', serde_common.LUACT_DHT_CMD_RESOLVE)
register_ctype('struct', 'luact_dht_cmd_resolve_txn', 'luact_dht_cmd_resolve_txn_t', serde_common.LUACT_DHT_CMD_RESOLVE_TXN)
register_ctype('struct', 'luact_dht_cmd_end_txn', 'luact_dht_cmd_end_txn_t', serde_common.LUACT_DHT_CMD_END_TXN)
register_ctype('struct', 'luact_dht_cmd_heartbeat_txn', 'luact_dht_cmd_heartbeat_txn', serde_common.LUACT_DHT_CMD_HEARTBEAT_TXN)

register_ctype('struct', 'luact_dht_gossip_replica_change', 'luact_dht_gossip_replica_change_t', serde_common.LUACT_DHT_GOSSIP_REPLICA_CHANGE)
register_ctype('struct', 'luact_dht_gossip_range_split', 'luact_dht_gossip_range_split_t', serde_common.LUACT_DHT_GOSSIP_RANGE_SPLIT)

-- base command
local base_mt = {}
base_mt.__gc = memory.free
function base_mt:set_txn(txn)
	if txn then
		ffi.copy(self:txn_p(), txn, ffi.sizeof('luact_dht_txn_t'))
		self.txn_f = 1
	else
		self.txn_f = 0
	end
end
function base_mt:get_txn()
	if self:has_txn() then
		return ffi.cast('luact_dht_txn_t *', self:txn_p())
	end
	return false
end
function base_mt:has_txn() 
	return self.txn_f ~= 0
end
function base_mt:create_versioned_value()
	return true
end
function base_mt:end_txn()
	return false
end
function base_mt:keylen()
	return self.kl
end
function base_mt:key()
	return self.p
end
function base_mt:end_key()
	return nil
end
function base_mt:end_keylen()
	return 0
end

-- get command
local get_mt = util.copy_table(base_mt)
get_mt.__index = get_mt
function get_mt.size(kl, txn)
	return ffi.sizeof('luact_dht_cmd_get_t') + kl + (txn and ffi.sizeof('luact_dht_txn_t') or 0)
end
function get_mt.new(kind, k, kl, timestamp, txn, consistent)
	local p = ffi.cast('luact_dht_cmd_get_t*', memory.alloc(get_mt.size(kl, txn)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	p.consistent = consistent and 1 or 0
	ffi.copy(p:key(), k, kl)
	p:set_txn(txn)
	return p
end
_M.get = get_mt.new
function get_mt:txn_p()
	return self.p + self.kl
end
function get_mt:__len()
	return get_mt.size(self.kl, self:has_txn())
end
function get_mt:create_versioned_value()
	return false
end
function get_mt:apply_to(storage, range)
	return range:exec_get(storage, self:key(), self.kl, self.timestamp, self:get_txn(), self.consistent ~= 0)
end
ffi.metatype('luact_dht_cmd_get_t', get_mt)

-- put command
local put_mt = util.copy_table(base_mt)
put_mt.__index = put_mt
function put_mt.size(kl, vl, txn)
	return ffi.sizeof('luact_dht_cmd_put_t') + kl + vl + (txn and ffi.sizeof('luact_dht_txn_t') or 0)
end
function put_mt.new(kind, k, kl, v, vl, timestamp, txn)
	local p = ffi.cast('luact_dht_cmd_put_t*', memory.alloc(put_mt.size(kl, vl, txn)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	p.vl = vl
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:val(), v, vl)
	p:set_txn(txn)
	return p
end
_M.put = put_mt.new
function put_mt:val()
	return self.p + self.kl
end
function put_mt:txn_p()
	return self.p + self.kl + self.vl
end
function put_mt:__len()
	return put_mt.size(self.kl, self.vl, self:has_txn())
end
function put_mt:apply_to(storage, range)
	return range:exec_put(storage, self:key(), self.kl, self:val(), self.vl, self.timestamp, self:get_txn())
end
ffi.metatype('luact_dht_cmd_put_t', put_mt)

-- delete command
local delete_mt = util.copy_table(base_mt)
delete_mt.__index = delete_mt
function delete_mt.size(kl, txn)
	return ffi.sizeof('luact_dht_cmd_delete_t') + kl + (txn and ffi.sizeof('luact_dht_txn_t') or 0)
end
function delete_mt.new(kind, k, kl, timestamp, txn)
	local p = ffi.cast('luact_dht_cmd_delete_t*', memory.alloc(delete_mt.size(kl, txn)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	ffi.copy(p:key(), k, kl)
	p:set_txn(txn)
	return p
end
_M.delete = delete_mt.new
function delete_mt:txn_p()
	return self.p + self.kl
end
function delete_mt:__len()
	return delete_mt.size(self.kl, self:has_txn())
end
function delete_mt:apply_to(storage, range)
	return range:exec_delete(storage, self:key(), self.kl, self.timestamp, self:get_txn())
end
ffi.metatype('luact_dht_cmd_delete_t', delete_mt)


-- cas command
local cas_mt = util.copy_table(base_mt)
cas_mt.__index = cas_mt
function cas_mt.size(kl, ol, nl, txn)
	return ffi.sizeof('luact_dht_cmd_cas_t') + kl + ol + nl + (txn and ffi.sizeof('luact_dht_txn_t') or 0)
end
function cas_mt.new(kind, k, kl, o, ol, n, nl, timestamp, txn)
	local p = ffi.cast('luact_dht_cmd_cas_t*', memory.alloc(cas_mt.size(kl, ol, nl, txn)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	p.ol = ol
	p.nl = nl
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:oldval(), o, ol)
	ffi.copy(p:newval(), n, nl)
	p:set_txn(txn)
	return p
end
_M.cas = cas_mt.new
function cas_mt:oldval()
	return self.p + self.kl
end
function cas_mt:newval()
	return self.p + self.kl + self.ol
end
function cas_mt:txn_p()
	return self.p + self.kl + self.ol + self.nl
end
function cas_mt:__len()
	return cas_mt.size(self.kl, self.ol, self.nl, self:has_txn())
end
function cas_mt:apply_to(storage, range)
	return range:exec_cas(storage, self:key(), self.kl, 
		self:oldval(), self.ol, self:newval(), self.nl, self.timestamp, self:get_txn())
end
ffi.metatype('luact_dht_cmd_cas_t', cas_mt)


-- scan command
local scan_mt = util.copy_table(base_mt)
scan_mt.__index = scan_mt
function scan_mt.size(kl, txn)
	return ffi.sizeof('luact_dht_cmd_scan_t') + kl + (txn and ffi.sizeof('luact_dht_txn_t') or 0)
end
function scan_mt.new(kind, k, kl, n, timestamp, txn, consistent)
	local p = ffi.cast('luact_dht_cmd_scan_t*', memory.alloc(scan_mt.size(kl, txn)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	p.n_process = n or 0
	p.consistent = consistent and 1 or 0
	ffi.copy(p:key(), k, kl)
	p:set_txn(txn)
	return p
end
_M.scan = scan_mt.new
function scan_mt:txn_p()
	return self.k + self.kl
end
function scan_mt:__len()
	return scan_mt.size(self.kl, self:has_txn())
end
function scan_mt:create_versioned_value()
	return false
end
function scan_mt:apply_to(storage, range)
	return range:exec_scan(storage, self:key(), self.kl, self.n_process, self.timestamp, self:get_txn(), self.consistent ~= 0)
end
ffi.metatype('luact_dht_cmd_scan_t', scan_mt)


-- resolve command
local resolve_mt = util.copy_table(base_mt)
resolve_mt.__index = resolve_mt
function resolve_mt.size(kl, ekl)
	return ffi.sizeof('luact_dht_cmd_resolve_t') + kl + ekl
end
function resolve_mt.new(kind, k, kl, ek, ekl, n, timestamp, txn)
	local p = ffi.cast('luact_dht_cmd_resolve_t*', memory.alloc(resolve_mt.size(kl, ekl)))
	p.kind = kind
	p.timestamp = timestamp
	p:set_txn(txn)
	p.kl = kl
	p.ekl = ekl
	p.n_process = n or 0
	ffi.copy(p:key(), k, kl)
	if ekl > 0 then
		ffi.copy(p:end_key(), ek, ekl)
	end
	return p
end
_M.resolve = resolve_mt.new
function resolve_mt:txn_p()
	return self.txn
end
function resolve_mt:has_txn()
	return true
end
function resolve_mt:set_txn(txn)
	ffi.copy(self.txn, txn, ffi.sizeof('luact_dht_txn_t'))
end
function resolve_mt:end_key()
	return (self.ekl > 0) and (self.p + self.kl) or nil
end
function resolve_mt:end_keylen()
	return self.ekl
end
function resolve_mt:create_versioned_value()
	return false
end
function resolve_mt:__len()
	return resolve_mt.size(self.kl, self.ekl)
end
function resolve_mt:apply_to(storage, range)
	return range:exec_resolve(storage, 
		self:key(), self.kl, self:end_key(), self.ekl, 
		self.n_process, self.timestamp, self:get_txn())
end
ffi.metatype('luact_dht_cmd_resolve_t', resolve_mt)


-- resolve_txn command
local resolve_txn_mt = util.copy_table(base_mt)
resolve_txn_mt.__index = resolve_txn_mt
function resolve_txn_mt.size()
	return ffi.sizeof('luact_dht_cmd_resolve_txn_t')
end
function resolve_txn_mt.new(kind, timestamp, txn, conflict_txn, how, cmd_ts)
	local p = ffi.cast('luact_dht_cmd_resolve_txn_t*', memory.alloc(resolve_txn_mt.size()))
	p.kind = kind
	p.timestamp = timestamp
	p.cmd_ts = cmd_ts
	ffi.copy(p.txn, txn, ffi.sizeof('luact_dht_txn_t'))
	ffi.copy(p.conflict_txn, conflict_txn, ffi.sizeof('luact_dht_txn_t'))
	p.how = how
	return p
end
_M.resolve_txn = resolve_txn_mt.new
function resolve_txn_mt:key()
	return nil
end
function resolve_txn_mt:keylen()
	return 0
end
function resolve_txn_mt:txn_p()
	return self.txn
end
function resolve_txn_mt:has_txn()
	return true
end
function resolve_txn_mt:set_txn(txn)
	ffi.copy(self.txn, txn, ffi.sizeof('luact_dht_txn_t'))
end
function resolve_txn_mt:create_versioned_value()
	return false
end
function resolve_txn_mt:__len()
	return resolve_txn_mt.size()
end
function resolve_txn_mt:apply_to(storage, range)
	return range:exec_resolve_txn(storage, self.timestamp, self.txn, self.conflict_txn, self.how, self.cmd_ts)
end
ffi.metatype('luact_dht_cmd_resolve_txn_t', resolve_txn_mt)


-- end_txn command
local heartbeat_txn_mt = util.copy_table(resolve_txn_mt)
heartbeat_txn_mt.__index = heartbeat_txn_mt
function heartbeat_txn_mt.new(kind, timestamp, txn)
	local p = ffi.cast('luact_dht_cmd_heartbeat_txn_t*', memory.alloc(heartbeat_txn_mt.size()))
	p.kind = kind
	p.timestamp = timestamp
	ffi.copy(p.txn, txn, ffi.sizeof('luact_dht_txn_t'))
	return p
end
_M.heartbeat_txn = heartbeat_txn_mt.new
function heartbeat_txn_mt:apply_to(storage, range)
	return range:exec_heartbeat_txn(storage, self.timestamp, self.txn)
end
ffi.metatype('luact_dht_cmd_heartbeat_txn_t', heartbeat_txn_mt)


-- end_txn command
local end_txn_mt = util.copy_table(resolve_txn_mt)
end_txn_mt.__index = end_txn_mt
function end_txn_mt.new(kind, timestamp, txn, commit)
	local p = ffi.cast('luact_dht_cmd_end_txn_t*', memory.alloc(end_txn_mt.size()))
	p.kind = kind
	p.timestamp = timestamp
	ffi.copy(p.txn, txn, ffi.sizeof('luact_dht_txn_t'))
	p.commit = commit and 1 or 0
	return p
end
_M.end_txn = end_txn_mt.new
function end_txn_mt:committed()
	return self.commit ~= 0 
end
function end_txn_mt:end_txn()
	return true
end
function end_txn_mt:apply_to(storage, range)
	return range:exec_end_txn(storage, self.timestamp, self.txn, self:committed())
end
ffi.metatype('luact_dht_cmd_end_txn_t', end_txn_mt)


-- merge command
local merge_mt = util.copy_table(base_mt)
merge_mt.__index = merge_mt
function merge_mt.size(kl, vl, ol, txn)
	return ffi.sizeof('luact_dht_cmd_merge_t') + kl + vl + ol + (txn and ffi.sizeof('luact_dht_txn_t') or 0)
end
function merge_mt.new(kind, k, kl, v, vl, o, ol, timestamp, txn)
	local p = ffi.cast('luact_dht_cmd_merge_t*', memory.alloc(merge_mt.size(kl, vl, ol, txn)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	p.vl = vl
	p.ol = ol
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:val(), v, vl)
	ffi.copy(p:op(), o, ol)
	p:set_txn(txn)
	return p
end
_M.merge = merge_mt.new
function merge_mt:val()
	return self.p + self.kl
end
function merge_mt:op()
	return self.p + self.kl + self.vl
end
function merge_mt:txn_p()
	return self.p + self.kl + self.vl + self.ol
end
function merge_mt:__len()
	return merge_mt.size(self.kl, self.vl, self.ol, self:has_txn())
end
function merge_mt:apply_to(storage, range)
	return range:exec_merge(storage, self:key(), self.kl, self:val(), self.vl, self:op(), self.ol, self.timestamp, self:get_txn())
end
ffi.metatype('luact_dht_cmd_merge_t', merge_mt)


-- watch command
local watch_mt = {}
watch_mt.__index = watch_mt
function watch_mt.size(kl, name_len, arg_len)
	return ffi.sizeof('luact_dht_cmd_watch_t') + kl + name_len + arg_len
end
function watch_mt.new(kind, k, kl, watcher, method, arg, arglen, timestamp)
	local p = ffi.cast('luact_dht_cmd_watch_t*', memory.alloc(watch_mt.size(kl, #method, arglen or 0)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	p.name_len = #method
	p.arg_len = arglen or 0
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:method(), method)
	if arg then
		ffi.copy(p:arg(), arg, arglen)
	end
	return p
end
_M.watch = watch_mt.new
function watch_mt:key()
	return self.p
end
function watch_mt:method()
	return self.p + self.kl
end
function watch_mt:arg()
	if self.arg_len > 0 then
		return self.p + self.kl + self.name_len
	end
end
function watch_mt:__len()
	return watch_mt.size(self.kl, self.name_len, self.arg_len)
end
function watch_mt:apply_to(storage, range)
	if self.arg_len > 0 then
		return range:exec_watch(storage, self:key(), self.kl, ffi.string(self:method(), self.name_len), 
			self:arg(), self.arg_len, self.timestamp)
	else
		return range:exec_watch(storage, self:key(), self.kl, ffi.string(self:method(), self.name_len,
			nil, nil, self.timestamp))
	end
end
ffi.metatype('luact_dht_cmd_watch_t', watch_mt)


-- split command
local split_mt = util.copy_table(base_mt)
split_mt.__index = split_mt
function split_mt.size(kl, txn)
	return ffi.sizeof('luact_dht_cmd_split_t') + kl + (txn and ffi.sizeof('luact_dht_txn_t') or 0)
end
function split_mt.new(kind, k, kl, timestamp, txn)
	local p = ffi.cast('luact_dht_cmd_split_t*', memory.alloc(split_mt.size(kl, txn)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	ffi.copy(p:key(), k, kl)
	p:set_txn(txn)
	return p
end
_M.split = split_mt.new
function split_mt:txn_p()
	return self.p + self.kl
end
function split_mt:__len()
	return split_mt.size(self.kl, self:has_txn())
end
function split_mt:apply_to(storage, range)
	return range:exec_split(storage, self:key(), self.kl, self.timestamp, self:get_txn())
end
ffi.metatype('luact_dht_cmd_split_t', split_mt)



-- gossip
_M.gossip = {}

-- gossip user message sub type
_M.GOSSIP_REPLICA_CHANGE = 1
_M.GOSSIP_RANGE_SPLIT = 2
_M.GOSSIP_ROOT_RANGE = 3

-- gossip for replica set change
local replica_change_mt = {}
replica_change_mt.__index = replica_change_mt
replica_change_mt.__gc = memory.free
function replica_change_mt.size(n_replica)
	return ffi.sizeof('luact_dht_gossip_replica_change_t') + ffi.sizeof('luact_uuid_t') * n_replica
end
function replica_change_mt.new(kind, key, replicas, n_replica)
	local p = ffi.cast('luact_dht_gossip_replica_change_t *', memory.alloc(replica_change_mt.size(n_replica)))
	p.key = key
	p.kind = kind
	p.n_replica = n_replica
	ffi.copy(p.replicas, replicas, ffi.sizeof('luact_uuid_t') * n_replica)
	for i=0, tonumber(n_replica) -1 do
		logger.warn('replica_change:', i, p.replicas[i])
	end
	return p
end
function replica_change_mt:__len()
	return replica_change_mt.size(self.n_replica)
end
ffi.metatype('luact_dht_gossip_replica_change_t', replica_change_mt)
function _M.gossip.replica_change(rng)
	for i=0, tonumber(rng.replica_available)-1 do
		if not uuid.valid(rng.replicas[i]) then
			exception.raise('fatal', 'invalid replica')
		end
	end
	return replica_change_mt.new(rng.kind, rng.start_key, rng.replicas, rng.replica_available)
end


-- gossip for range split
local range_split_mt = {}
range_split_mt.__index = range_split_mt
range_split_mt.__gc = memory.free
function range_split_mt.new(kind, key, split_at)
	local p = memory.alloc_typed('luact_dht_gossip_range_split_t')
	p.key = key
	p.kind = kind
	p.split_at = split_at
	return p
end
function range_split_mt:__len()
	return ffi.sizeof(self)
end
ffi.metatype('luact_dht_gossip_range_split_t', range_split_mt)
function _M.gossip.range_split(rng, split_at)
	return range_split_mt.new(rng.kind, rng.start_key, split_at)
end


-- gossip for root range
function _M.gossip.root_range(rng)
	return ffi.string(rng, #rng)
end


return _M
