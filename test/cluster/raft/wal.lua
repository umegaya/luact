local luact = require 'luact.init'

luact.start({
	cache_dir = "/tmp/luact",
	n_core = 1, exclusive = true,
}, function ()
local luact = require 'luact.init'
local fs = require 'pulpo.fs'
local ok,r = xpcall(function ()
	local wal = require 'luact.cluster.raft.wal'
	local rdbstore = require 'luact.cluster.store.rocksdb'
	local serde = require 'luact.serde'
	local util = require 'pulpo.util'

	fs.rmdir('/tmp/luact/rocksdb/testdb2')
	local store = rdbstore.new('/tmp/luact/rocksdb/testdb2', 'test')
	local opts = {
		log_compaction_margin = 3,
	}
	local w = wal.new({hoge = 'fuga'}, store, serde[serde.kind.serpent], opts)

	local logs = {
		{"a", 1}, -- 1
		{"b", 1},
		{"c", 1},
		{"d", 1},
		{"a", 1}, -- 5
		{"d", 2}, 
		{"c", 4},
		{"e", 5},
	}
	local logs2 = {
		{"x", 4},
		{"y", "8"}, -- 10
		{"z", "5"},
		{"w", "4"},
		{"y", 4},
		{"x", "9"},
		{"w", "6"}, -- 15
		{"z", 4},
	}
	local state = {
		{"a", "b", "c", x = { y = 1 }}
	}
	local function new_fsm()
		return setmetatable({}, {
			__index = {
				apply = function (self, data)
					self[data[1]] = data[2]
				end
			}
		})
	end

	w:write(nil, 1, logs)
	assert(w:last_index() == #logs, "its first insert, so last index should be same as # of logs inserted")
	for idx=1,#logs do
		local l = w:at(idx)
		assert(l.term == 1 and l.index == idx, "term should be same as given to write(), and index should be correctly assigned")
		assert(logs[idx][1] == l.log[1] and logs[idx][2] == l.log[2], "at same index, same logs should be stored")
	end
	local sub = w:logs_from(4)
	assert(#sub == 5, "# of returned logs should be 5 (8 - 4 + 1) because it also includes given index")
	for idx=1,#sub do
		assert(logs[idx+3][1] == sub[idx].log[1] and logs[idx+3][2] == sub[idx].log[2], "log contents should be matched with same index")
	end
	w:delete_logs(3)
	assert(w:last_index() == #logs, "last index should not change by log deletion")
	for idx=1,3 do
		assert(nil == w:at(idx), "nil should return when at() call for deleted index:"..tostring(w:at(idx)).."|"..tostring(idx))
	end
	for idx=4,#logs do
		local l = w:at(idx)
		assert(logs[idx][1] == l.log[1] and logs[idx][2] == l.log[2], "at same index, same logs should be stored")
	end
	w:write(nil, 2, logs2)
	for idx=1,3 do
		assert(nil == w:at(idx), "nil should return when at() call for deleted index")
	end
	for idx=4,#logs do
		local l = w:at(idx)
		assert(l.term == 1 and l.index == idx, "term should be same as given to write(), and index should be correctly assigned")
		assert(logs[idx][1] == l.log[1] and logs[idx][2] == l.log[2], "at same index, same logs should be stored")
	end
	for idx=1,#logs2 do
		local logindex = idx+#logs
		local l = w:at(logindex)
		assert(l.term == 2 and l.index == logindex, "term should be same as given to write(), and index should be correctly assigned")
		assert(logs2[idx][1] == l.log[1] and logs2[idx][2] == l.log[2], "at same index, same logs should be stored")
	end
	w:compaction(#logs+#logs2)
	local last_removed = #logs+#logs2-opts.log_compaction_margin
	for idx=1,last_removed do
		assert(nil == w:at(idx), "nil should return when at() call for deleted index")
	end
	for logidx=last_removed+1,#logs+#logs2 do
		local idx = logidx - #logs
		local l = w:at(logidx)
		assert(l.term == 2 and l.index == logidx, "term should be same as given to write(), and index should be correctly assigned")
		assert(logs2[idx][1] == l.log[1] and logs2[idx][2] == l.log[2], "at same index, same logs should be stored")
	end

	w:write_state(state)
	local st = w:read_state()
	-- print((require 'serpent').dump(state), (require 'serpent').dump(st))
	local ret, k = util.table_equals(state, st)
	assert(ret, "same object which given to write_state() should returns:"..tostring(k).."|"..tostring(state[k]).."|"..tostring(st[k]))
end, function (e)
	logger.error('err', e, debug.traceback())
end)

fs.rmdir('/tmp/luact/rocksdb/testdb2')

luact.stop()
end)


return true