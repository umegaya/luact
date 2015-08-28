local luact = require 'luact.init'
local uuid = require 'luact.uuid'
local clock = require 'luact.clock'
local serde = require 'luact.serde'
local router = require 'luact.router'
local actor = require 'luact.actor'

local range = require 'luact.cluster.dht.range'
local cmd = require 'luact.cluster.dht.cmd'

local txncoord = require 'luact.storage.txncoord'

local pulpo = require 'pulpo.init'
local event = require 'pulpo.event'
local util = require 'pulpo.util'
local memory = require 'pulpo.memory'
local socket = require 'pulpo.socket'
local tentacle = require 'pulpo.tentacle'
local fs = require 'pulpo.fs'

local _M = {}
local dhp_map = {}
local range_manager
local range_gossiper


-- cdefs 
ffi.cdef [[
typedef struct luact_dht {
	uint8_t kind, padd[3];
	double timeout;
} luact_dht_t;
]]


-- table wrapper
local dht_wrap_mt = {}
dht_wrap_mt.__index = dht_wrap_mt
local txn_key = "\0\0txn"
local dht_key = "\0\0dht"
local opt_key = "\0\0opt"
function new_dht_wrap(dht, opt)
	return setmetatable({
		[dht_key] = dht,
		[opt_key] = opt or {},
	}, dht_wrap_mt)
end
function dht_wrap_mt:__index(k)
	return self[dht_key]:rawget(k, #k, self[txn_key], self[opt_key].consistent, self[opt_key].timeout)
end
function dht_wrap_mt:__newindex(k, v)
	self[dht_key]:put(k, #k, v, #v, self[txn_key], self[opt_key].timeout)
end


-- dht object
local dht_mt = {}
dht_mt.__index = dht_mt
function dht_mt:init(name, operation_timeout, kind)
	self.kind = range_manager:bootstrap_kind(name, kind)
	self.timeout = operation_timeout
end
function dht_mt:destroy(truncate)
	range_manager:shutdown(self.kind, truncate)
	memory.free(self)
end
function dht_mt:range_of(k, kl)
	return range_manager:find(k, kl, self.kind)
end
function dht_mt:get(k, txn, consistent, timeout)
	return self:rawget(k, #k, txn, consistent, timeout)
end
function dht_mt:put(k, v, txn, timeout)
	return self:rawput(k, #k, v, #v, txn, timeout)
end
function dht_mt:cas(k, oldval, newval, txn, timeout)
	return self:rawcas(k, #k, oldval, #oldval, newval, #newval, txn, timeout)
end
function dht_mt:merge(k, v, op, txn, timeout)
	return self:rawmerge(k, #k, v, #v, op, #op, txn, timeout)
end
function dht_mt:rawget(k, kl, txn, consistent, timeout)
	return self:range_of(k, kl):rawget(k, kl, txn, consistent, timeout or self.timeout)
end
function dht_mt:rawput(k, kl, v, vl, txn, timeout)
	return self:range_of(k, kl):rawput(k, kl, v, vl, txn, timeout or self.timeout)
end
function dht_mt:rawcas(k, kl, oldval, ovl, newval, nvl, txn, timeout)
	return self:range_of(k, kl):cas(k, kl, oldval, ovl, newval, nvl, txn, timeout or self.timeout)
end
function dht_mt:rawmerge(k, kl, v, vl, o, ol, txn, timeout)
	return self:range_of(k, kl):rawmerge(k, kl, v, vl, o, ol, txn, timeout or self.timeout)
end
function dht_mt:txn(proc, ...)
	txncoord.run_txn(proc, ...)
end


-- module functions
local default_opts = {
	n_replica = range.DEFAULT_REPLICA,
	storage = "rocksdb",
	datadir = luact.DEFAULT_ROOT_DIR,
	root_range_send_interval = 30,
	replica_maintain_interval = 1.0,
	collect_garbage_interval = 60 * 60,
	range_size_max = 64 * 1024 * 1024,
	gossip_port = 8008,
	range_prefetch_count = 8,
	txn_heartbeat_interval = 5.0, -- 5sec
	max_clock_skew = 0.25, -- 250 msec
	replica_selector = 'different_thread', -- allow same node, if thread_id is different.
}
local function check_and_fill_config(opts)
	opts.datadir = fs.path(opts.datadir, tostring(pulpo.thread_id), "dht")
	if type(opts.replica_selector) == 'string' then
		local s = opts.replica_selector
		if s == 'different_thread' then 
			opts.replica_selector = function (mid, tid, selected)
				if selected then
					for i=1,#selected do
						local tmp_mid, tmp_tid = uuid.machine_id(selected[i]), uuid.thread_id(selected[i])
						if mid == tmp_mid and tid == tmp_tid then
							return false
						end
					end
				end
				return true
			end
		elseif s == 'different_node' then
			opts.replica_selector = function (mid, tid, selected)
				if selected then
					for i=1,#selected do
						local tmp_mid = uuid.machine_id(selected[i])
						if mid == tmp_mid then
							return false
						end
					end
				end
				return true
			end
		else  
			-- if you want to more special logic like different rack or datacenter, 
			-- you check it by seeing machine_id (actually ipv4 address in VLAN, so you can give some rule for them) 
			-- or, save metadata (like {machine_id}/dc == dc1) when you boot your node.
			-- for which way, custom replica selector is useful.
			opts.replica_selector = require(s)
		end
	end
end
function _M.initialize(parent_address, opts)
	opts = util.merge_table(default_opts, opts)
	local nodelist = parent_address and {actor.root_of(parent_address, 1)} or nil
	-- initialize module wide shared variables
	check_and_fill_config(opts)
	range_manager = range.get_manager(nodelist, opts.datadir, opts)
	while not range_manager:initialized() do
		io.write(pulpo.thread_id); io.stdout:flush()
		luact.clock.sleep(1)
	end
	io.write('\n')
	txncoord.initialize(range_manager, opts.txncoord)
	logger.notice('waiting dht module initialization finished')
end

function _M.manager_actor()
	return range.manager_actor()
end

function _M.finalize()
	range.destroy_manager()
end

function _M.new(name, timeout)
	return new_dht_wrap(_M.new_raw(name, timeout))
end
function _M.new_raw()
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
