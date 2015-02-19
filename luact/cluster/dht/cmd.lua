local ffi = require 'ffiex.init'
local util = require 'pulpo.util'
local memory = require 'pulpo.memory'
local lamport = require 'pulpo.lamport'
local uuid = require 'luact.uuid'
local serde_common = require 'luact.serde.common'

local key = require 'luact.cluster.dht.key'

local _M = {}

ffi.cdef [[
typedef struct luact_dht_cmd_get {
	pulpo_hlc_t timestamp;
	uint8_t kind, padd;
	uint16_t kl;
	char k[0];
} luact_dht_cmd_get_t;

typedef struct luact_dht_cmd_put {
	pulpo_hlc_t timestamp;
	uint8_t kind, padd;
	uint16_t kl;
	uint32_t vl;
	char kv[0];	
} luact_dht_cmd_put_t;

typedef struct luact_dht_cmd_cas {
	pulpo_hlc_t timestamp;
	uint8_t kind, padd;
	uint16_t kl;
	uint32_t ol;
	uint32_t nl;
	char kon[0];		
} luact_dht_cmd_cas_t;

typedef struct luact_dht_cmd_merge {
	pulpo_hlc_t timestamp;
	uint8_t kind, padd;
	uint16_t kl;
	uint32_t vl;
	char kv[0];		
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
	uint8_t kind, padd;
	uint16_t kl;
	char k[0];
} luact_dht_cmd_split_t;

typedef struct luact_dht_cmd_scan {
	pulpo_hlc_t timestamp;
	uint8_t kind, padd;
	uint16_t kl;
	char k[0];	
} luact_dht_cmd_scan_t;

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
register_ctype('struct', 'luact_dht_cmd_cas', 'luact_dht_cmd_cas_t', serde_common.LUACT_DHT_CMD_CAS)
register_ctype('struct', 'luact_dht_cmd_merge', 'luact_dht_cmd_merge_t', serde_common.LUACT_DHT_CMD_MERGE)
register_ctype('struct', 'luact_dht_cmd_watch', 'luact_dht_cmd_watch_t', serde_common.LUACT_DHT_CMD_WATCH)
register_ctype('struct', 'luact_dht_cmd_split', 'luact_dht_cmd_split_t', serde_common.LUACT_DHT_CMD_SPLIT)
register_ctype('struct', 'luact_dht_cmd_scan', 'luact_dht_cmd_scan_t', serde_common.LUACT_DHT_CMD_SCAN)

register_ctype('struct', 'luact_dht_gossip_replica_change', 'luact_dht_gossip_replica_change_t', serde_common.LUACT_DHT_GOSSIP_REPLICA_CHANGE)
register_ctype('struct', 'luact_dht_gossip_range_split', 'luact_dht_gossip_range_split_t', serde_common.LUACT_DHT_GOSSIP_RANGE_SPLIT)

-- get command
local get_mt = {}
get_mt.__index = get_mt
get_mt.__gc = memory.free
function get_mt.size(kl)
	return ffi.sizeof('luact_dht_cmd_get_t') + kl
end
function get_mt.new(kind, k, kl, timestamp)
	local p = ffi.cast('luact_dht_cmd_get_t*', memory.alloc(get_mt.size(kl)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	ffi.copy(p:key(), k, kl)
	return p
end
_M.get = get_mt.new
function get_mt:key()
	return self.k
end
function get_mt:__len()
	return get_mt.size(self.kl)
end
function get_mt:apply_to(storage, range)
	return range:exec_get(storage, self:key(), self.kl, self.timestamp)
end
ffi.metatype('luact_dht_cmd_get_t', get_mt)

-- put command
local put_mt = {}
put_mt.__index = put_mt
put_mt.__gc = memory.free
function put_mt.size(kl, vl)
	return ffi.sizeof('luact_dht_cmd_put_t') + kl + vl
end
function put_mt.new(kind, k, kl, v, vl, timestamp)
	local p = ffi.cast('luact_dht_cmd_put_t*', memory.alloc(put_mt.size(kl, vl)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	p.vl = vl
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:val(), v, vl)
	return p
end
_M.put = put_mt.new
function put_mt:key()
	return self.kv
end
function put_mt:val()
	return self.kv + self.kl
end
function put_mt:__len()
	return put_mt.size(self.kl, self.vl)
end
function put_mt:apply_to(storage, range)
	return range:exec_put(storage, self:key(), self.kl, self:val(), self.vl, self.timestamp)
end
ffi.metatype('luact_dht_cmd_put_t', put_mt)


-- cas command
local cas_mt = {}
cas_mt.__index = cas_mt
cas_mt.__gc = memory.free
function cas_mt.size(kl, ol, nl)
	return ffi.sizeof('luact_dht_cmd_cas_t') + kl + ol + nl
end
function cas_mt.new(kind, k, kl, o, ol, n, nl, timestamp)
	local p = ffi.cast('luact_dht_cmd_cas_t*', memory.alloc(cas_mt.size(kl, ol, nl)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	p.ol = ol
	p.nl = nl
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:oldval(), o, ol)
	ffi.copy(p:newval(), n, nl)
	return p
end
_M.cas = cas_mt.new
function cas_mt:key()
	return self.kon
end
function cas_mt:oldval()
	return self.kon + self.kl
end
function cas_mt:newval()
	return self.kon + self.kl + self.ol
end
function cas_mt:__len()
	return cas_mt.size(self.kl, self.ol, self.nl)
end
function cas_mt:apply_to(storage, range)
	return range:exec_cas(storage, self:key(), self.kl, self:oldval(), self.ol, self:newval(), self.nl, self.timestamp)
end
ffi.metatype('luact_dht_cmd_cas_t', cas_mt)


-- get command
local scan_mt = {}
scan_mt.__index = scan_mt
scan_mt.__gc = memory.free
function scan_mt.size(kl)
	return ffi.sizeof('luact_dht_cmd_scan_t') + kl
end
function scan_mt.new(kind, k, kl, timestamp)
	local p = ffi.cast('luact_dht_cmd_scan_t*', memory.alloc(scan_mt.size(kl)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	ffi.copy(p:key(), k, kl)
	return p
end
_M.scan = scan_mt.new
function scan_mt:key()
	return self.k
end
function scan_mt:__len()
	return scan_mt.size(self.kl)
end
function scan_mt:apply_to(storage, range)
	return range:exec_scan(storage, self:key(), self.kl, self.timestamp)
end
ffi.metatype('luact_dht_cmd_scan_t', scan_mt)


-- merge command
local merge_mt = {}
merge_mt.__index = merge_mt
merge_mt.__gc = memory.free
function merge_mt.size(kl, vl)
	return ffi.sizeof('luact_dht_cmd_merge_t') + kl + vl
end
function merge_mt.new(kind, k, kl, v, vl, timestamp)
	local p = ffi.cast('luact_dht_cmd_merge_t*', memory.alloc(get_mt.size(kl, vl)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	p.vl = vl
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:val(), v, vl)
	return p
end
_M.merge = merge_mt.new
function merge_mt:key()
	return self.kv
end
function merge_mt:val()
	return self.kv + self.kl
end
function merge_mt:__len()
	return cas_mt.size(self.kl, self.vl)
end
function merge_mt:apply_to(storage, range)
	return range:exec_merge(storage, self:key(), self.kl, self:val(), self.vl, self.timestamp)
end
ffi.metatype('luact_dht_cmd_merge_t', merge_mt)


-- watch command
local watch_mt = {}
watch_mt.__index = watch_mt
watch_mt.__gc = memory.free
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
local split_mt = {}
split_mt.__index = split_mt
split_mt.__gc = memory.free
function split_mt.size(kl)
	return ffi.sizeof('luact_dht_cmd_split_t') + kl
end
function split_mt.new(kind, k, kl, timestamp)
	local p = ffi.cast('luact_dht_cmd_split_t*', memory.alloc(split_mt.size(kl)))
	p.kind = kind
	p.timestamp = timestamp
	p.kl = kl
	ffi.copy(p:key(), k, kl)
	return p
end
_M.split = split_mt.new
function split_mt:key()
	return self.k
end
function split_mt:__len()
	return split_mt.size(self.kl)
end
function split_mt:apply_to(storage, range)
	return range:exec_split(storage, self:key(), self.kl, self.timestamp)
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
