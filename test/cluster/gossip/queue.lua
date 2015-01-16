local luact = require 'luact.init'
local tools = require 'test.tools.cluster'


tools.start_luact(1, nil, function ()
	local memory = require 'pulpo.memory'
	local pulpo = require 'pulpo.init'
	local uuid = require 'luact.uuid'
	local nodelist = require 'luact.cluster.gossip.nodelist'
	local protocol = require 'luact.cluster.gossip.protocol'
	local queue = require 'luact.cluster.gossip.queue'
	local n_retransmit = 5
	-- mock
	local mship = {
		retransmit = function (self)
			return n_retransmit
		end,
		handle_user_message = function ()
		end,
		handle_node_change = function ()
		end,
	}

	local l = nodelist.new(8888)
	l:add_self()
	
	local q = queue.new(1024, 1024)
	q:push(mship, protocol.new_change(l:self()))
	assert(q:used() == 1, "element should be pushed")
	l:self().state = nodelist.suspect
	q:push(mship, protocol.new_change(l:self()))
	assert(q:used() == 1, "push for same node should not be entried twice because of try_invalidate")
	local pop_count = 0
	while true do
		local vec, len = q:pop(mship, 1024)
		if not len then
			assert(pop_count == 5, "because the element in queue has retransmit == 5, so should be popped 5 fimes")
			break
		end
		assert(len == 1, "there is only 1 element so it should returns 1 length array")
		local v = vec[0]
		local s = protocol.from_ptr(v.iov_base)
		assert(ffi.typeof(s) == ffi.typeof('struct luact_gossip_proto_sys *'))
		pop_count = pop_count + 1
	end

	n_retransmit = 1

	local bufs = {}
	local total = 0
	for i=1,1000 do
		bufs[i] = 'queue element '..i
		local buf_p = memory.strdup(bufs[i])
		local u = protocol.new_user(buf_p, #bufs[i], 0)
		total = total + u:length()
		q:push(mship, u)
	end

	print('pop mtu test----')
	local pop_total = 0	
	local cnt = 1000
	local first = true
	local appeared = {}
	while true do
		local vec, len = q:pop(mship, 1024)
		if not len then
			break
		end
		for i=0,len-1,2 do
			local v = vec[i]
			local u = protocol.from_ptr(v.iov_base)
			assert(ffi.typeof(u) == ffi.typeof('struct luact_gossip_proto_user *'))
			pop_total = pop_total + u:length()
			-- first pop, queue buffer is not sorted, so packet is appeard FILO order
			if first then
				assert(not appeared[cnt], "same packet should not appear twice")
				appeared[cnt] = true
				-- print(vec[i+1].iov_base, '['..ffi.string(vec[i + 1].iov_base)..']', '['..bufs[cnt]..']')
				assert(ffi.string(vec[i + 1].iov_base) == bufs[cnt], "packet data should be correct")
				-- print('iovlen', vec[i + 1].iov_len, bufs[cnt], #bufs[cnt], i + 1)
				assert(vec[i + 1].iov_len == #bufs[cnt], "packet data should be correct")
				cnt = cnt - 1
			else
			-- after 1st pop, queue buffer sorted by unstable sort algorithm, so appeard order is uncertain.
				local str = ffi.string(vec[i + 1].iov_base)
				assert(#str == vec[i + 1].iov_len, "packet data should be correct")
				cnt = tonumber(str:match('[0-9]+$'))
				assert(not appeared[cnt], "same packet should not appear twice")
				appeared[cnt] = true
				assert(str == bufs[cnt], "packet data should be correct")
				assert(vec[i + 1].iov_len == #bufs[cnt], "packet data should be correct")				
			end
		end
		first = false
	end
	print(total, pop_total)
	assert(total == pop_total, "push/pop length should match")

	queue.destroy(q)
end)



return true
