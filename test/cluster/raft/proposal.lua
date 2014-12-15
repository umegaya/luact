local luact = require 'luact.init'

luact.start({
	cache_dir = "/tmp/luact",
	n_core = 1, exclusive = true,
	arbiter = false, -- don't use default vid 
}, function ()
local luact = require 'luact.init'
local ok,r = xpcall(function ()
	local proposal = require 'luact.cluster.raft.proposal'
	local util = require 'pulpo.util'
	local clock = require 'luact.clock'

	local logs = setmetatable({
		{"a", 1, index = 1}, -- 1
		{"b", 1, index = 2},
		{"c", 1, index = 3},
		{"d", 1, index = 4},
		{"e", 1, index = 5}, -- 5
		{"d", 2, index = 6}, 
		{"c", 4, index = 7},
		{"e", 5, index = 8},
		{"x", 4, index = 9},
		{"y", "8", index = 10}, -- 10
		{"z", "5", index = 11},
		{"w", "4", index = 12},
		{"y", 4, index = 13},
		{"x", "9", index = 14},
		{"w", "6", index = 15}, -- 15
		{"z", 4, index = 16},
		{"g", 6, index = 17},
		{"h", 7, index = 18},
		{"i", 8, index = 19}
	}, {
		__index = {
			at = function (self, i)
				return self[i]
			end,
		}
	})

	local p = proposal.new(logs, 16)
	local body = setmetatable({ check = {} }, {
		__index = {
			accepted = function (self)
				for idx, a in ipairs(p.accepted) do
					-- print('accepted!!!!', a.index, a[1], a[2])
					assert(not self.check[a.index], "logs should be never accepted twice")
					self.check[a.index] = true
					self[a[1]] = a[2]
					p.accepted[idx] = nil
				end
			end,
		}
	})
	local actor = luact(body)

	p:add(3, 1, 8)
	p:add(2, 9, 16)
	assert(p.progress.header.n_size == 16)
	local s
	for i=1,8 do
		s = p.progress:at(i)
		assert(s.quorum == 3 and s.current == 0, "quorum should be same as specified at proposal:add() and size initialized")
	end
	for i=9,16 do
		s = p.progress:at(i)
		assert(s.quorum == 2 and s.current == 0, "quorum should be same as specified at proposal:add() and size initialized")
	end
	
	p:add(3, 17, 19)
	assert(p.progress.header.n_size == 32)
	
	s = p.progress:at(1)
	p:range_commit(actor, 1, 1)
	assert(s.current == 1, "if committed, size should increase")
	assert((not s:granted()) and (not body.a), "if committed and still not reach to quorum, logs should not be processed")

	p:range_commit(actor, 1, 1)
	assert(s.current == 2, "if committed, size should increase")
	assert((not s:granted()) and (not body.a), "if committed and still not reach to quorum, logs should not be processed")

	p:range_commit(actor, 1, 1)
	clock.sleep(0.1)
	assert(s.current == 3, "if committed, size should increase")
	assert(body.check[1] and body.a == 1, "if committed and reach to quorum, logs should be processed")
	
	local log
	p:range_commit(actor, 1, 16)
	p:range_commit(actor, 1, 16)
	for i=2,8 do
		log = logs[i]
		assert(not body.check[i] and (not body[log[1]]), "first 8 progress should not reach to quorum and logs should not be processed")
	end
	for i=9,16 do
		s = p.progress:at(i)
		log = logs[i]
		assert(s:valid() and s:granted() and (not body[log[1]]), "next 8 progress should reach to quorum. but logs should not be processed")
	end
	p:range_commit(actor, 1, 8)
	clock.sleep(0.1)
	for i=1,16 do
		log = logs[i]
		assert(body.check[i] and body[log[1]], "first 16 progress should reach to quorum. and logs should be processed")
	end
	for i=17,19 do
		s = p.progress:at(i)
		log = logs[i]
		assert(s:valid() and (not s:granted()) and (not body[log[1]]), "last 3 progress should not reach to quorum. and logs should not be processed")
	end

	local last = { a = 1, b = 1, c = 4, d = 2, e = 5, x = "9", y = 4, z = 4, w = "6", check = body.check}
	assert(util.table_equals(last, body), "log should be applied correctly")

end, function (e)
	logger.error('err', e)
end)

luact.stop()
end)

return true
