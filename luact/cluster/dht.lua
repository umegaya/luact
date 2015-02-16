local luact = require 'luact.init'
local uuid = require 'luact.uuid'
local clock = require 'luact.clock'
local serde = require 'luact.serde'
local router = require 'luact.router'
local actor = require 'luact.actor'

local range = require 'luact.cluster.dht.range'

local pulpo = require 'pulpo.init'
local event = require 'pulpo.event'
local util = require 'pulpo.util'
local memory = require 'pulpo.memory'
local socket = require 'pulpo.socket'
local tentacle = require 'pulpo.tentacle'
local fs = require 'pulpo.fs'

local _M = {}
local dhp_map = {}


-- cdefs 
ffi.cdef [[
typedef struct luact_dht {
	uint8_t kind, padd[3];
	double timeout;
} luact_dht_t;
]]


-- dht object
local dht_mt = {}
dht_mt.__index = dht_mt
function dht_mt:init(name, operation_timeout)
	self.kind = range.bootstrap(name)
	self.timeout = operation_timeout
end
function dht_mt:destroy(truncate)
	range.shutdown(self.kind, truncate)
	memory.free(self)
end
function dht_mt:range_of(k, kl)
	return range.find(k, kl, self.kind)
end
function dht_mt:__index(k)
	-- default is consistent read
	return self:rawget(k, #k, true)
end
function dht_mt:__newindex(k, v)
	return self:put(k, #k, v, #v)
end
function dht_mt:get(k, consistent, timeout)
	return self:rawget(k, #k, consistent, timeout)
end
function dht_mt:rawget(k, kl, consistent, timeout)
	return self:range_of(k, kl):rawget(k, kl, consistent, timeout or self.timeout)
end
function dht_mt:put(k, kl, v, vl, timeout)
	return self:range_of(k, kl):rawput(k, kl, v, vl, timeout or self.timeout)
end
function dht_mt:cas(k, oldval, newval, timeout)
	return self:rawcas(k, #k, oldval, #oldval, newval, #newval, timeout)
end
function dht_mt:merge(k, v, timeout)
	return self:rawmerge(k, #k, v, #v, timeout)
end
function dht_mt:watch(k, watcher, method, timeout)
	return self:rawwatch(k, #k, watcher, method, timeout or self.timeout)
end
function dht_mt:rawcas(k, kl, oldval, ovl, newval, nvl, timeout)
	return self:range_of(k, kl):cas(k, kl, oldval, ovl, newval, nvl, timeout or self.timeout)
end
function dht_mt:rawmerge(k, kl, v, vl, timeout)
	return self:range_of(k, kl):rawmerge(k, kl, v, vl, timeout or self.timeout)
end
function dht_mt:rawwatch(k, kl, watcher, method, timeout)
	return self:range_of(k, kl):watch(k, kl, watcher, method, timeout or self.timeout)
end
function dht_mt:new_txn()
	assert(false, "TBD")
end


-- module functions
local default_opts = {
	replica = range.DEFAULT_REPLICA,
	storage = "rocksdb",
	datadir = luact.DEFAULT_ROOT_DIR,
}
local function configure_datadir(opts)
	if not opts.datadir then
		exception.raise('invalid', 'config', 'options must contain "datadir"')
	end
	return fs.path(opts.datadir, tostring(pulpo.thread_id))
end
function _M.initialize(root_range, opts)
	opts = util.merge_table(default_opts, opts)
	range.initialize(root_range, configure_datadir(opts), opts)
end

function _M.finalize()
	range.finalize()
end

function _M.new(name, timeout)
	name = name or range.DEFAULT_FAMILY
	local r = dht_map[name]
	if not r then
		r = memory.alloc_typed('luact_dht_t')
		r:init(name, timeout)
		dht_map[name] = r
	end
	return r
end

function _M.destroy(dht, truncate)
	local name = range.family_name_by_kind(dht.kind)
	dht:destroy(truncate)
	dht_map[name] = nil
end

function _M.truncate(dht)
	_M.destroy(dht, true)
end

return _M
