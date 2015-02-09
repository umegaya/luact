local luact = require 'luact.init'

luact.start({
	datadir = "/tmp/luact",
	n_core = 1, exclusive = true,
	arbiter = false, 
}, function ()
local luact = require 'luact.init'
local fs = require 'pulpo.fs'
local ok,r = xpcall(function ()
	local snapshot = require 'luact.cluster.raft.snapshot'
	local serde = require 'luact.serde'
	local uuid = require 'luact.uuid'
	local util = require 'pulpo.util'

	fs.rmdir('/tmp/luact/snapshot')
	local ss = snapshot.new('/tmp/luact/snapshot', serde[serde.kind.serpent])

	local logs = {
		{"a", 1}, -- 1
		{"b", 1},
		{"c", 1},
		{"d", 1},
		{"a", 1}, -- 5
		{"d", 2}, 
		{"c", 4},
		{"e", 5},
		{"x", 4},
		{"y", "8"}, -- 10
		{"z", "5"},
		{"w", "4"},
		{"y", 4},
		{"x", "9"},
		{"w", "6"}, -- 15
		{"z", 4},
	}
	local function new_state(term, replica_set, idx)
		return setmetatable({
			term = term or 1, 
			last_applied_idx = idx or 111,
			replica_set = replica_set or {"1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4"},
		}, {
			__index = {
				read_snapshot_header = function (self, hd)
					self.term = hd.term
					self.last_applied_idx = hd.index
					for i=1,hd.n_replica do
						table.insert(self.replica_set, hd.replicas[i - 1])
					end
				end,
				write_snapshot_header = function (self, hd)
					if hd.n_replica < #self.replica_set then
						hd = hd:realloc(#self.replica_set)
					end
					hd.term = self.term
					hd.index = self.last_applied_idx
					hd.n_replica = #self.replica_set
					for i=1,#self.replica_set do
						hd.replicas[i - 1] = uuid.debug_create_id(self.replica_set[i], i)
					end
					return hd	
				end,
			}
		})
	end
	local init_state = {
		[1] = "a", [2] = "b", [3] = "c", 
		a = 1, b = 2, c = 3, d = 4
	}
	local function new_fsm(data)
		return setmetatable(data or {}, {
			__index = {
				snapshot = function (self, sr, rb)
					sr:pack(rb, self)
				end,
				restore = function (self, sr, rb)
					local obj = sr:unpack(rb)
					for k,v in pairs(obj) do
						self[k] = v
					end
				end,
			}
		})
	end
	local st = new_state(4)
	local fsm = new_fsm(init_state)

	for i=1,16 do
		st.last_applied_idx = i
		init_state.a = init_state.a + 100
		ss:write(fsm, st)
	end

	local fsm2 = new_fsm()
	ss:restore(fsm2)
	local index, term = ss:last_index_and_term()
	assert(index == 16 and term == 4, "index and term should match")
	assert(util.table_equals(fsm, fsm2), "state should be recovered correctly")
end, function (e)
	logger.error('err', e)
	os.exit(-2)
end)

-- fs.rmdir('/tmp/luact/snapshot')

luact.stop()
end)

return true