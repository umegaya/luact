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
function dht_mt:range_of(k, kl)
	return range.find(k, kl, self.kind)
end
function dht_mt:__index(k)
	return self:get(k, #k)
end
function dht_mt:__newindex(k, v)
	return self:put(k, #k, v, #v)
end
function dht_mt:get(k, kl, consistent, timeout)
	return self:range_of(k, kl):rawget(k, kl, consistent, timeout or self.timeout)
end
function dht_mt:put(k, kl, v, vl, timeout)
	return self:range_of(k, kl):rawput(k, kl, v, vl, timeout or self.timeout)
end
function dht_mt:cas(k, oldval, newval, timeout)
	return self:rawcas(k, #k, oldval, #oldval, newval, #newval, timeout or self.timeout)
end
function dht_mt:watch(k, notice_to, method, timeout)
	return self:rawwatch(k, #k, notice_to, method, timeout or self.timeout)
end
function dht_mt:rawcas(k, kl, oldval, ovl, newval, nvl, timeout)
	return self:range_of(k, kl):cas(k, kl, oldval, ovl, newval, nvl, timeout or self.timeout)
end
function dht_mt:rawwatch(k, kl, notice_to, method, timeout)
	return self:range_of(k, kl):watch(k, kl, notice_to, method, timeout or self.timeout)
end
function dht_mt:new_txn()
	assert(false, "TBD")
end


-- module functions
function _M.initialize(root_range, opts)
	range.initialize(root_range, opts)
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

function _M.destroy(dht)
end

function _M.truncate(dht)
	-- TODO : who really need that?
end

return _M
