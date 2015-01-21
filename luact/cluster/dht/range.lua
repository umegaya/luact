local luact = require 'luact.init'
local uuid = require 'luact.uuid'
local clock = require 'luact.clock'
local serde = require 'luact.serde'
local router = require 'luact.router'
local actor = require 'luact.actor'

local pulpo = require 'pulpo.init'
local event = require 'pulpo.event'
local util = require 'pulpo.util'
local memory = require 'pulpo.memory'
local socket = require 'pulpo.socket'
local tentacle = require 'pulpo.tentacle'
local fs = require 'pulpo.fs'

local raft = require 'luact.cluster.raft'
local key = require 'luact.cluster.dht.key'
local cache = require 'luact.cluster.dht.cache'
local cmd = require 'luact.cluster.dht.cache'


-- module share variable
local _M = {}
local db
local root_range
local column_families = {
	lookup = {}
}
local range_caches = {}


-- constant
_M.MAX_BYTE = 64 * 1024 * 1024
_M.INITIAL_BYTE = 1 * 1024 * 1024
_M.DEFAULT_REPLICA = 3
_M.DEFAULT_FAMILY = '__data__'


-- cdefs
ffi.cdef [[
typedef struct luact_dht_range {
	luact_dht_key_t start_key;
	luact_dht_key_t end_key;
	uint8_t n_replica, kind, padd[2];
	luact_uuid_t replicas[0];		//arbiter actors' uuid
} luact_dht_range_t;
]]


-- range
local range_mt = {}
range_mt.__index = range_mt
function range_mt.alloc(n_replica)
	local p = ffi.cast('luact_dht_range_t*', 
		memory.alloc(ffi.sizeof('luact_dht_range_t') + (n_replica * ffi.sizeof('luact_uuid_t')))
	)
	p.n_replica = n_replica
	return p
end
function range_mt:init(start_key, end_key, kind)
	ffi.copy(self.start_key, start_key, ffi.sizeof(self.start_key))
	ffi.copy(self.start_key, end_key, ffi.sizeof(self.end_key))
	self.kind = kind
end
function range_mt:fin()
	-- TODO : consider when range need to be removed, and do correct finalization
end
function range_mt:arbiter_id()
	return ffi.string(self.start_key.p, self.start_key.length)
end
-- operation to range
function range_mt:get(k, consistent, timeout)
	return self:rawget(k, #k, consistent, timeout)
end
function range_mt:rawget(k, kl, consistent, timeout)
	return self.replicas[0]:read(cmd.rawget(self.kind, k, kl), consistent, timeout)
end
function range_mt:put(k, v, timeout)
	return self:rawput(k, #k, v, #v, timeout)
end
function range_mt:rawput(k, kl, v, vl, timeout)
	return self.replicas[0]:write(cmd.rawput(self.kind, k, kl, v, vl), timeout)
end
function range_mt:cas(k, kl, ov, ovl, nv, nvl, timeout)
	return self.replicas[0]:write(cmd.cas(self.kind, k, kl, ov, ovl, nv, nvl), timeout)
end
function range_mt:watch(k, kl, notice_to, method, timeout)
	return self.replicas[0]:write(cmd.watch(self.kind, k, kl, notice_to, method), timeout)
end
function range_mt:split(range_key, timeout)
	return self.replicas[0]:write(cmd.split(self.kind, range_key), timeout)
end
-- call from raft module
function range_mt:apply(cmd)
	local s = column_families[cmd.kind]
	-- TODO : implement various operation
	return cmd:apply_to(s, self)
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
ffi.metatype('luact_dht_range_t', range_mt)


-- module functions
function _M.initialize(root, opts)
	root_range = root -- from gossiper
	db = (require ('luact.cluster.store.'..opts.storage)).new(opts.storage_dir, "dht")
	_M.KIND_META1 = _M.new_family('__meta1__')
	_M.KIND_META2 = _M.new_family('__meta2__')
	-- you can add more meta range without any code change.
	_M.KIND_DEFAULT = _M.new_family(_M.DEFAULT_FAMILY)
	_M.NUM_REPLICA = opts.replica or _M.DEFAULT_REPLICA
end

function _M.new_family(name)
	if #column_families >= 255 then
		exception.raise('invalid', 'cannot create new family: full')
	end
	local c = db:column_family(name)
	table.insert(column_families, c)
	local kind = #column_families
	table.insert(range_caches, cache.new(kind))
	column_families.lookup[name] = c
	return kind
end

function _M.finalize()
	db:fin()
end

function _M.bootstrap(name)
	local kind = _M.new_family(name)
	local r = _M.new(key.MIN, key.MAX, kind)
	range_caches[kind]:add(r)
	return kind
end

function _M.new(start_key, end_key, kind)
	kind = kind or _M.KIND_DEFAULT
	local r = range_mt.alloc(_M.NUM_REPLICA)
	r:init(start_key, end_key, kind)
	range_caches[kind]:add(r)
	return r
end

function _M.find(k, kl, kind)
	local r
	kind = kind or _M.KIND_DEFAULT
	if kind >= _M.KIND_DEFAULT then
		r = range_caches[kind]:find(key)
		if not r then
			r = _M.find(key, _M.KIND_META2)
			r = r:get(key)
			range_caches[kind]:add(r)
		end
	elseif kind > _M.KIND_META1 then
		r = range_caches[kind]:find(key)
		if not r then
			r = _M.find(key, kind - 1)
			r = r:get(key)
			range_caches[kind]:add(r)
		end
	else
		r = root_range:get(key)
	end
	return r
end

function _M.destroy(r)
	r:fin()
	memory.free(r)
end

return _M
