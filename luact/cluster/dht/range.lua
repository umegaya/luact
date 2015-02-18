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
local fs = require 'pulpo.fs'

local raft = require 'luact.cluster.raft'
local gossip = require 'luact.cluster.gossip'
local key = require 'luact.cluster.dht.key'
local cache = require 'luact.cluster.dht.cache'
local cmd = require 'luact.cluster.dht.cmd'

local storage_module


-- module share variable
local _M = {}
local persistent_db
local root_range
local column_families = {
	lookup = {}
}
local range_caches = {}


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
	v, vl = storage:rawget(k, kl) -- resolve all logs for this key (thus it processed above merge now)
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
function range_mt.fsm_factory(rng)
	return rng
end
function range_mt:init(start_key, end_key, kind)
	self.start_key = start_key
	self.end_key = end_key
	self.kind = kind
end
function range_mt:add_replica(remote)
	remote = remote or actor.root_of(nil, luact.thread_id)
	self.replicas[self.replica_available] = remote.arbiter(self:arbiter_id(), range_mt.fsm_factory, self)
	self.replica_available = self.replica_available + 1
end
function range_mt:fin()
	-- TODO : consider when range need to be removed, and do correct finalization
	assert(false, "TBD")
end
function range_mt:arbiter_id()
	return self:metakey()
end
function range_mt:metakey()
	return make_metakey(self.kind, ffi.string(self.start_key.p, self.start_key.length))
end
function range_mt:check_replica()
	if self.replica_available < _M.NUM_REPLICA then
		exception.raise('invalid', 'dht', 'not enough replica', self.n_replica)
	end
end
-- operation to range
function range_mt:get(k, consistent, timeout)
	return self:rawget(k, #k, consistent, timeout)
end
function range_mt:rawget(k, kl, consistent, timeout)
	self:check_replica()
	return self.replicas[0]:read(cmd.get(self.kind, k, kl), consistent, timeout)
end
function range_mt:put(k, v, timeout)
	return self:rawput(k, #k, v, #v, timeout)
end
function range_mt:rawput(k, kl, v, vl, timeout)
	self:check_replica()
	return self.replicas[0]:write(cmd.put(self.kind, k, kl, v, vl), timeout)
end
function range_mt:merge(k, v, timeout)
	return self:rawmerge(k, #k, v, #v, timeout)
end
function range_mt:rawmerge(k, kl, v, vl, timeout)
	self:check_replica()
	return self.replicas[0]:write(cmd.merge(self.kind, k, kl, v, vl), timeout)
end
function range_mt:cas(k, ov, nv, timeout)
	local oval, ol = ov or nil, ov and #ov or 0
	local nval, nl = nv or nil, nv and #nv or 0
	local cas = storage_module.op_cas(ov, nv, nil, ovl, nvl)
	return self:rawcas(k, #k, oval, ol, nval, nl, timeout)
end
function range_mt:rawcas(k, kl, ov, ovl, nv, nvl, timeout)
	self:check_replica()
	return self.replicas[0]:write(cmd.cas(self.kind, k, kl, ov, ovl, nv, nvl), timeout)
end
function range_mt:watch(k, kl, watcher, method, timeout)
	self:check_replica()
	return self.replicas[0]:write(cmd.watch(self.kind, k, kl, watcher, method), timeout)
end
function range_mt:split(range_key, timeout)
	self:check_replica()
	return self.replicas[0]:write(cmd.split(self.kind, range_key), timeout)
end
-- actual processing on replica node of range
function range_mt:exec_get(storage, k, kl)
	local v, vl = storage:rawget(k, kl)
	local p = ffi.string(v, vl)
	memory.free(v)
	return p
end
function range_mt:exec_put(storage, k, kl, v, vl)
	return storage:rawput(k, kl, v, vl)
end
function range_mt:exec_merge(storage, k, kl, v, vl)
	return storage:rawmerge(k, kl, v, vl)
end
range_mt.cas_result = memory.alloc_typed('bool')
local function sync_cas(storage, k, kl, o, ol, n, nl)
	range_mt.cas_result[0] = false
	local cas = storage_module.op_cas(o, n, range_mt.cas_result, ol, nl)
	sync_merge(storage, k, kl, cas, #cas)
	return range_mt.cas_result[0]
end
function range_mt:exec_cas(storage, k, kl, o, ol, n, nl)
	return sync_cas(storage, k, kl, o, ol, n, nl)
end
function range_mt:exec_watch(storage, k, kl, watcher, method, arg, alen)
	assert(false, "TBD")
end
function range_mt:exec_split(storage, k, kl)
	assert(false, "TBD")
end
function range_mt:column_family()
	return column_families[self.kind]
end
-- call from raft module
function range_mt:apply(cmd)
	assert(self.kind == cmd.kind)
	local cf = self:column_family()
	-- TODO : implement various operation
	return cmd:apply_to(cf, self)
end
function range_mt:metadata()
	assert(false, "TBD")
end
function range_mt:snapshot(sr, rb)
	assert(false, "TBD")
end
function range_mt:restore(sr, rb)
	assert(false, "TBD")
end
function range_mt:attach()
	logger.info('range', 'attached')
end
function range_mt:detach()
	logger.info('range', 'detached')
end
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
ffi.metatype('luact_dht_range_t', range_mt)


-- module functions
function _M.initialize(parent_address, datadir, opts)
	storage_module = require ('luact.storage.'..opts.storage) 
	persistent_db = storage_module.open(datadir)
	range_mt.sync_write_opts = storage_module.new_write_opts({ sync = true })
	_M.NUM_REPLICA = opts.n_replica
	if parent_address then -- memorize root_range. all other ranges can be retrieve from it
		_M.gossiper = luact.root_actor.gossiper(opts.gossip_port, {
			nodelist = {actor.root_of(parent_address, 1)},
			delegate = function ()
				return (require 'luact.cluster.dht.range').delegate
			end
		})
		logger.info('wait for bootstrap dht cluster')
		while not _M.delegate.initialized do
			io.write('.')
			luact.clock.sleep(1.0)
		end
		io.write('\n')
	else -- create initial range hirerchy structure manualy 
		local meta2, default, meta2_key, default_key, root_cf, meta2_cf, default_cf
		-- create storage for initial dht setting with bootstrap mode
		root_cf = _M.new_family(_M.KIND_META1, _M.META1_FAMILY, true)
		meta2_cf = _M.new_family(_M.KIND_META2, _M.META2_FAMILY, true)
		default_cf = _M.new_family(_M.KIND_VID, _M.VID_FAMILY, true)
		-- create root range
		root_range = _M.new(key.MIN, key.MAX, _M.KIND_META1)
		-- put initial meta2 storage into root_range
		meta2 = _M.new(key.MIN, key.MAX, _M.KIND_META2)
		meta2_key = meta2:metakey()
		root_cf:rawput(meta2_key, #meta2_key, ffi.cast('char *', meta2), ffi.sizeof(meta2))
		-- put initial default storage into meta2_range
		default = _M.new(key.MIN, key.MAX, _M.KIND_VID)
		default_key = default:metakey()
		meta2_cf:rawput(default_key, #default_key, ffi.cast('char *', default), ffi.sizeof(default))
	end
	return root_range
end

_M.delegate = {}
function _M.delegate:memberlist_event(type, ...)

end

function _M.new_family(kind, name, cluster_bootstrap)
	if #column_families >= 255 then
		exception.raise('invalid', 'cannot create new family: full')
	end
	local c = column_families.lookup[name]
	if not c then
		c = persistent_db:column_family(name)
		column_families[kind] = c
		range_caches[kind] = cache.new(kind)
		column_families.lookup[name] = c
	end
	if cluster_bootstrap then
		local syskey = make_syskey(_M.SYSKEY_CATEGORY_KIND, tostring(kind))
		if not sync_cas(c, syskey, #syskey, nil, 0, name, #name) then
			exception.raise('fatal', 'initial kind of dht cannot registered', kind, name)
		end
	end
	return c
end

function _M.family_name_by_kind(kind)
	local cf = column_families[kind]
	for k,v in pairs(column_families.lookup) do
		if v == cf then
			return k
		end
	end
	return nil
end

function _M.finalize()
	range_caches = {}
	root_range = nil
	persistent_db:fin()
end

-- create new kind of dht which name is *name*, 
-- it is caller's responsibility to give unique value for *kind*.
-- otherwise it fails.
function _M.bootstrap(kind, name)
	local syskey = make_syskey(_M.SYSKEY_CATEGORY_KIND, tostring(kind))
	if root_range:cas(syskey, nil, name) then
		_M.new_family(id, name)
		local r = _M.new(key.MIN, key.MAX, kind)
		range_caches[kind]:add(r)
		return kind
	end
end

-- shutdown specified kind of dht
-- TODO : do following steps to execute cluster-wide shutdown
-- 1. write something to root_range indicate this kind of dht is shutdown
-- 2. leader of raft cluster of root_range send special gossip broadcast to notify shutdown
-- 3. if node receive shutdown gossip message and have any replica of range which have data of this kind of dht, 
--     remove them.
function _M.shutdown(kind, truncate)
	assert(false, "TBD")
end

function _M.new(start_key, end_key, kind)
	kind = kind or _M.KIND_VID
	local r = range_mt.alloc(_M.NUM_REPLICA)
	r:init(start_key, end_key, kind)
	r:add_replica()
	range_caches[kind]:add(r)
	return r
end

-- find range which contains key (k, kl)
-- search original kind => KIND_META2 => KIND_META1
function _M.find(k, kl, kind)
	kind = kind or _M.KIND_VID
	local r = range_caches[kind]:find(k, kl)
	if not r then
		k = make_metakey(kind, ffi.string(k, kl))
		kl = #k
		if kind >= _M.KIND_VID then
			-- make unique key over all key in kind >= _M.KIND_VID
			if not r then
				-- find range from meta ranges
				r = _M.find(k, kl, _M.KIND_META2)
				if r then
					r = r:rawget(k, kl, true)
				end
			end
		elseif kind > _M.KIND_META1 then
			if not r then
				-- find range from top level meta ranges
				r = _M.find(k, kl, kind - 1)
				if r then
					r = r:rawget(k, kl, true)
				end
			end
		else -- KIND_META1
			r = root_range:rawget(k, kl, true)
		end
		if r then 
			range_caches[kind]:add(r) 
		end
	end
	return r
end

function _M.destroy(r)
	r:fin()
	memory.free(r)
end

return _M
