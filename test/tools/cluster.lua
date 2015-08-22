local luact = require 'luact.init'
local clock = require 'luact.clock'
local ffi = require 'ffiex.init'
local util = require 'pulpo.util'
local fs = require 'pulpo.fs'
local pulpo = require 'pulpo.init'
local thread = require 'pulpo.thread'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local _M = {}

ffi.cdef [[
	typedef struct luact_thread_payload {
		int t;
		char *ptr;
		size_t len;
	} luact_thread_payload_t;
	typedef struct luact_thread_latch {
		int size;
		int *values;
	} luact_thread_latch_t;
]]

local thread_payload_index = {}
thread_payload_mt = {
	__index = thread_payload_index,
}
local TYPE_NIL = 0
local TYPE_FUNCTION = 1
local TYPE_STRING = 2
local TYPE_NUMBER = 3
local TYPE_BOOLEAN = 4
function thread_payload_index:encode(payload)
	if type(payload) == 'function' or type(payload) == 'string' then
		local enc = util.encode_proc(payload)
		self.ptr = memory.strdup(enc)
		self.len = #enc
		self.t = type(payload) == 'function' and TYPE_FUNCTION or TYPE_STRING
	elseif type(payload) == 'number' or type(payload) == 'boolean' then
		local enc = tostring(payload)
		self.t = type(payload) == 'number' and TYPE_NUMBER or TYPE_BOOLEAN
		self.ptr = memory.strdup(enc)
		self.len = #enc
	elseif type(payload) == 'nil' then
		self.t = TYPE_NIL
	end
end
function thread_payload_index:decode()
	if self.t == TYPE_FUNCTION or self.t == TYPE_STRING then
		return util.decode_proc(ffi.string(self.ptr, self.len))
	elseif self.t == TYPE_NUMBER then 
		return tonumber(ffi.string(self.ptr, self.len))
	elseif self.t == TYPE_BOOLEAN then
		return self.len == 4
	elseif self.t == TYPE_NIL then
		return nil
	end
end
function thread_payload_index:fin()
	if self.ptr ~= ffi.NULL then
		memory.free(self.ptr)
	end
end
ffi.metatype('luact_thread_payload_t', thread_payload_mt)

function _M.start_luact(n_core, opts, proc)
	local ptr = memory.alloc_typed('luact_thread_payload_t')
	ptr:encode(proc)
	opts = util.merge_table({
		datadir = "/tmp/luact",
		n_core = n_core, exclusive = true,
		arg = ptr, 
		arbiter = arbiter, 
		dht = {
			gossip_port = false, -- disable dht. vid will run in local mode
		}, 
	}, opts or {})
	luact.start(opts, function (p)
		xpcall(function ()
			local luact = require 'luact.init'
			local ffi = require 'ffiex.init'
			local pulpo = require 'pulpo.init'
			local util = require 'pulpo.util'
			local fs = require 'pulpo.fs'
			local tools = require 'test.tools.cluster'
			local ok,r = xpcall(function ()
				fs.rmdir('/tmp/luact/'..tostring(pulpo.thread_id))
				local fn = ffi.cast('luact_thread_payload_t*', p):decode()
				fn()
			end, function (e)
				logger.error(e, debug.traceback())
				os.exit(-2)
			end)
			luact.stop()
		end, function (e)
			logger.fatal('start luact: fails', e)
			os.exit(-2)
		end)
	end)
	ptr:fin()
	return true
end

function _M.start_local_cluster(n_core, leader_thread_id, fsm_factory, proc)
	local ptr = memory.alloc_typed('luact_thread_payload_t', 4)
	ptr[0]:encode(fsm_factory)
	ptr[1]:encode(proc)
	ptr[2]:encode(leader_thread_id)
	ptr[3]:encode(n_core)
	luact.start({
		datadir = "/tmp/luact",
		n_core = n_core, exclusive = true,
		arg = ptr, 
		dht = {
			gossip_port = false, -- disable dht. vid will run in local mode
		}, 
	}, function (p)
		local luact = require 'luact.init'
		local raft = require 'luact.cluster.raft'
		local ffi = require 'ffiex.init'
		local pulpo = require 'pulpo.init'
		local util = require 'pulpo.util'
		local fs = require 'pulpo.fs'
		local clock = require 'luact.clock'
		local actor = require 'luact.actor'
		local uuid = require 'luact.uuid'
		local tools = require 'test.tools.cluster'
		fs.rmdir('/tmp/luact/'..tostring(pulpo.thread_id))
		local ok,r = xpcall(function ()
			local ptr = ffi.cast('luact_thread_payload_t*', p)
			local leader_thread_id = ptr[2]:decode()
			local n_core = ptr[3]:decode()
			local arb, rft, rs
			if pulpo.thread_id == leader_thread_id then
				local factory = ptr[0]:decode()
				arb = actor.root_of(nil, pulpo.thread_id).arbiter('test_group', factory, { initial_node = true }, pulpo.thread_id)
				logger.info('arb1', arb)
				clock.sleep(2.5) -- wait for this thread become raft leader (max election timeout (2.0) + margin (0.5))
				rft = raft._find_body('test_group')
				assert(uuid.equals(arb, rft:leader()), "this is only raft object to bootstrap, so should be leader")
				rs = {}
				for i=1,n_core do
					local replica = actor.root_of(nil, i).arbiter('test_group', factory, nil, i)
					assert(replica, "arbiter should be created")
					table.insert(rs, replica)
				end
				rft:add_replica_set(rs)
			else
				while not (rft and arb) do
					clock.sleep(0.1)
					arb = actor.root_of(nil, pulpo.thread_id).arbiter('test_group')
					rft = raft._find_body('test_group')
				end
				logger.info('arb2', arb)
				 -- wait for replica_set is replicated.
				rs = rft:replica_set()
				while #rs < n_core do
					clock.sleep(0.1)
					rs = rft:replica_set()
				end
			end
			rft:probe(function (r)
				assert(r.state:has_enough_nodes_for_election(), "all nodes should be election-ready")
			end)
			local found
			for i=1,n_core do
				if uuid.equals(rs[i], arb) then
					found = true
					break
				end
			end
			assert(found, "each thread's uuid should be included in replica set:"..tostring(arb))
			local fn = ffi.cast('luact_thread_payload_t*', p)[1]:decode()
			fn(rft, pulpo.thread_id)
		end, function (e)
			logger.fatal(e, debug.traceback())
			os.exit(-2)
		end)
		luact.stop()
	end)
	ptr:fin()
	return true
end

local thread_latch_index = {}
local thread_latch_mt = {
	__index = thread_latch_index
}
function thread_latch_index:wait(value, thread_id)
	local p = self.values
	thread_id = thread_id or pulpo.thread_id
	p[thread_id - 1] = value
	while true do
		local finished = true
		for i=0, self.size - 1 do
			if p[i] < value then
				finished = false
			end
		end
		if finished then
			break
		end
		clock.sleep(1.0)
	end	
end
ffi.metatype('luact_thread_latch_t', thread_latch_mt)
function _M.create_latch(name, num_thread)
	return thread.shared_memory(name, function ()
		local p = memory.alloc_fill_typed('luact_thread_latch_t')
		p.size = num_thread
		p.values = memory.alloc_fill_typed('int', num_thread)
		return 'luact_thread_latch_t', p
	end, num_thread)
end

-- create dummy arbiter for single thread mode
local delay_actor
function _M.delay_emurator() 
	if not delay_actor then
		delay_actor = luact({
			c = function ()
			end,
		})
	end
	return delay_actor
end
function _M.use_dummy_arbiter(on_read, on_write)
	local raft = require 'luact.cluster.raft'
	local range = require 'luact.cluster.dht.range'
	local range_arbiter_bodies = {}
	local mact = raft.manager_actor()
	function luact.root.arbiter(id, func, opts, rng)
		local rm = range.get_manager()
		rng = rm:create_fsm_for_arbiter(rng)
		local storage = rng:partition()
		-- logger.notice('arbiter_id = ', ('%q'):format(id), range_arbiters[id])
		local a = range_arbiter_bodies[id]
		if not a then
			local body = {
				read = function (self, timeout, ...)
					if on_read then on_read(self, timeout, ...) end
					return rng:fetch_state(...)
				end,
				write = function (self, logs, timeout, dictatorial)
					if on_write then on_write(self, logs, timeout, dictatorial) end
					return rng:apply(logs[1])
				end,
				replica_set = function (self)
					return self.rs
				end,
				leader = function (self)
					return self.ldr
				end,
				change_replica_set = function (self)
					rng:debug_add_replica(mact)
				end,
				rs = {},
			}
			body.ldr = mact
			range_arbiter_bodies[id] = body
			luact.tentacle(function ()
				-- emurate delay of election. 
				-- difference of luact.clock.sleep is, luact.clock.sleep may ignore causality order of function call when luact.root.arbiter called via actor messaging.
				-- e.g. luact.root.arbiter call should returns before change_replica_set is called. but if queue has plenty of un-processed response, 
				-- response of luact.root.arbiter is far more after than fixed wait time of luact.clock.sleep. then body:change_replica_set called before
				-- body:change_replica_set called, that is not true for actual arbiter environment. 
				--
				-- ranges in bootstrap uses this thread's rpc queue to achieve finish election and change_replica_set, 
				-- for ranges created by split, if communication with another node is more smooth than local rpc queue interaction, change_replica_set may call 
				-- before luact.root.arbiter returns to caller (so, range_mt:start_raft_group called luact.root.arbiter directly)
				--
				-- if you use rpc to make delay, this rpc surely returns after luact.root.arbiter returns (because both used same rpc queue, causality order preserved)
				_M.delay_emurator().c() -- make delay
				body:change_replica_set()
			end)
		else
			assert(false, "same arbiter should not called")
		end
		return mact
	end
	function raft._find_body(id)
		return range_arbiter_bodies[id]
	end
	function raft.rpc(kind, id, ...)
		local rft = range_arbiter_bodies[id]
		if not rft then
			exception.raise('raft', 'not_found', id)
		end
		if kind == raft.APPEND_ENTRIES then
			return rft:append_entries(...)
		elseif kind == raft.REQUEST_VOTE then
			return rft:request_vote(...)
		elseif kind == raft.INSTALL_SNAPSHOT then
			return rft:install_snapshot(...)
		else
			exception.raise('raft', 'invalid_rpc', kind)
		end
	end
	function raft.manager_actor()
		return mact
	end
end

local function test_err_handle(e)
	return exception.new_with_bt('runtime', debug.traceback(), e)
end
function _M.test_runner(name, proc, ctor, dtor)
	local r 
	if ctor then
		r = {xpcall(ctor, test_err_handle)}
		if not r[1] then
			error(r[2])
		end
	end
	local args
	if r then
		args = util.copy_table(r)
		r = {xpcall(proc, test_err_handle, unpack(r, 2))}
	else
		r = {xpcall(proc, test_err_handle)}
	end
	if not r[1] then
		error(r[2])
	end
	if dtor then
		if args then
			r = {xpcall(dtor, test_err_handle, unpack(args, 2))}
		else
			r = {xpcall(dtor, test_err_handle)}
		end
		if not r[1] then
			error(r[2])
		end
	end
	logger.notice(name, 'success')
end

function _M.new_fsm(thread_id)
	return setmetatable({}, {
		__index = {
			metadata = function (self)
				return {'this', 'is', meta = 'data', ['for'] = thread_id}
			end,
			snapshot = function (self, sr, rb)
				sr:pack(rb, self)
			end,
			restore = function (self, sr, rb)
				local obj = sr:unpack(rb)
				for k,v in pairs(obj) do
					self[k] = v
				end
			end,
			change_replica_set = function (self, type, self_affected, replica_set)
			end,
			apply = function (self, data)
			-- logger.warn('apply', data[1], data[2])
				self[ data[1] ] = data[2]
			end,
			attach = function (self)
			end,
			detach = function (self)
			end,
		}
	})
end

function _M.is_true_within(fn, duration)
	local total, wait = 0, 0.01
	while total < duration do
		if fn() then
			return true
		end
		luact.clock.sleep(wait)
		total = total + wait
		wait = wait * 2
	end
	return false
end

return _M
