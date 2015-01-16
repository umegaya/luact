local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_luact(1, nil, function ()
	local memory = require 'pulpo.memory'
	local pulpo = require 'pulpo.init'
	local uuid = require 'luact.uuid'
	local nodelist = require 'luact.cluster.gossip.nodelist'
	local protocol = require 'luact.cluster.gossip.protocol'

	local l = nodelist.new(8888)
	l:add_self()
	
	local buf = 'user broadcat'
	local buf_p = memory.strdup(buf)
	local u = ffi.cast('void *', protocol.new_user(buf_p, #buf, 0))
	u = protocol.from_ptr(u)
	assert(ffi.typeof(u) == ffi.typeof('struct luact_gossip_proto_user *'), 'protocol module should recognize type of byte array correctly')
	assert(ffi.string(u.buf_p) == buf, 'packet data should set correctly')
	assert(u.len == #buf, 'packet data should set correctly')

	local self = l:self()
	local s = ffi.cast('void *', protocol.new_change(self))
	s = protocol.from_ptr(s)
	assert(ffi.typeof(s) == ffi.typeof('struct luact_gossip_proto_sys *'), 'protocol module should recognize type of byte array correctly')
	assert(s.thread_id == self.thread_id, 'packet data should set correctly')
	assert(s.machine_id == self.machine_id, 'packet data should set correctly')
	assert(s.protover == self.protover, 'packet data should set correctly')
	assert(s.state == self.state, 'packet data should set correctly')
end)



return true
