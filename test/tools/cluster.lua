local luact = require 'luact.init'
local ffi = require 'ffiex.init'
local util = require 'pulpo.util'
local fs = require 'pulpo.fs'
local memory = require 'pulpo.memory'
local _M = {}

ffi.cdef [[
	typedef struct luact_thread_payload {
		int t;
		char *ptr;
		size_t len;
	} luact_thread_payload_t;
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

function _M.start_luact(n_core, arbiter, proc)
	local ptr = memory.alloc_typed('luact_thread_payload_t')
	ptr:encode(proc)
	luact.start({
		cache_dir = "/tmp/luact",
		n_core = n_core, exclusive = true,
		arg = ptr, 
		arbiter = arbiter, 
	}, function (p)
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
			logger.error('err', e, debug.traceback())
		end)
		luact.stop()
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
		cache_dir = "/tmp/luact",
		n_core = n_core, exclusive = true,
		arg = ptr, 
		arbiter = arbiter, 
	}, function (p)
		local luact = require 'luact.init'
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
			if pulpo.thread_id == leader_thread_id then
				local factory = ptr[0]:decode()
				arb = actor.root_of(nil, pulpo.thread_id).arbiter('test_group', factory, nil, pulpo.thread_id)
				logger.info('arb1', arb)
				clock.sleep(2)
				assert(uuid.equals(arb, arb:leader()), "this is only raft object to bootstrap, so should be leader")
				local replica_set = {}
				for i=1,n_core do
					local replica = actor.root_of(nil, i).arbiter('test_group', factory, nil, i)
					assert(replica, "arbiter should be created")
					table.insert(replica_set, replica)
				end
				arb:add_replica_set(replica_set)
			else
				clock.sleep(2 + 2)
				arb = actor.root_of(nil, pulpo.thread_id).arbiter('test_group')
				logger.info('arb2', arb)
			end
			local rs = arb:replica_set()
			assert(#rs == n_core, "# of replica_set should be "..n_core..":"..#rs)
			local found
			for i=1,n_core do
				if uuid.equals(rs[i], arb) then
					found = true
					break
				end
			end
			assert(found, "each thread's uuid should be included in replica set:"..tostring(arb))
			local fn = ffi.cast('luact_thread_payload_t*', p)[1]:decode()
			fn(arb, pulpo.thread_id)
		end, function (e)
			logger.error('err', e, debug.traceback())
		end)
		luact.stop()
	end)
	ptr:fin()
	return true
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
			apply = function (self, data)
			logger.warn('apply', data[1], data[2])
				self[ data[1] ] = data[2]
			end,
			attach = function (self)
			end,
			detach = function (self)
			end,
		}
	})
end

return _M
