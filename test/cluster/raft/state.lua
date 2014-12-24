local luact = require 'luact.init'

luact.start({
	cache_dir = "/tmp/luact",
	n_core = 1, exclusive = true,
	arbiter = false, 
}, function ()
local luact = require 'luact.init'
local ok,r = xpcall(function ()
	local state = require 'luact.cluster.raft.state'
	local wal = require 'luact.cluster.raft.wal'
	local snapshot = require 'luact.cluster.raft.snapshot'
	local replicator = require 'luact.cluster.raft.replicator'
	local rdbstore = require 'luact.cluster.store.rocksdb'
	local serde = require 'luact.serde'
	local uuid = require 'luact.uuid'
	local clock = require 'luact.clock'
	local util = require 'pulpo.util'
	local fs = require 'pulpo.fs'
	local memory = require 'pulpo.memory'
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
				apply = function (self, data)
					self[data[1]] = data[2]
				end,
				attach = function (self)
				end,
				detach = function (self)
				end,
			}
		})
	end
	local function new_actor_body(p, st)
		return setmetatable({ check = {} }, {
			__index = {
				accepted = function (self)
					for idx, log in ipairs(p.accepted) do
						assert(not self.check[log.index], "logs should be never accepted twice")
						self.check[idx] = true
						-- proceed commit log index
						local ok, r = st:apply(log)
						assert(ok, "apply log should not fail:"..tostring(r))
						p.accepted[idx] = nil
					end
				end,
			}
		})
	end

	-- replace replicator.new to mockup
	function replicator.new(leader_actor, actor, state)
		local p = memory.alloc_fill_typed('luact_raft_replicator_t')
		p:init(state)
		return p
	end
	local opts = {
		logsize_snapshot_threshold = 4,
		initial_proposal_size = 16,
		log_compaction_margin = 2, 
		snapshot_file_preserve_num = 3, 
		election_timeout_sec = 0.15,
		heartbeat_timeout_sec = 1.0,
		proposal_timeout_sec = 5.0,
		serde = "serpent",
		storage = "rocksdb",
		debug_node_kind = "leader", 
		workdir = luact.DEFAULT_ROOT_DIR,
	}
	-- add replica set => 1
	local logs = {
		{"a", 1}, -- 2
		{"b", 1},
		{"c", 1},
		{"d", 1}, -- 5
		{"a", 1}, 
		{"d", 2}, 
		{"c", 4},
		{"e", 5},
	}
	-- remove replica set => 10
	local logs2 = {
		{"x", 4}, 
		{"y", "8"}, 
		{"z", "5"},
		{"w", "4"},
		{"y", 4}, -- 15
		{"x", "9"},
		{"w", "6"}, 
		{"z", 4}, -- 18
	}
	local replica_set = {
		uuid.debug_create_id("1.1.1.1", 1),
		uuid.debug_create_id("2.2.2.2", 2),
		uuid.debug_create_id("3.3.3.3", 3),
		uuid.debug_create_id("4.4.4.4", 4),
	}
	local addr_thread_id_pairs = {
		{uuid.addr(replica_set[1]), uuid.thread_id(replica_set[1])},
		{uuid.addr(replica_set[2]), uuid.thread_id(replica_set[2])},
		{uuid.addr(replica_set[3]), uuid.thread_id(replica_set[3])},
		{uuid.addr(replica_set[4]), uuid.thread_id(replica_set[4])},
	}

	local SNAPSHOT_DIR = fs.abspath('tmp', 'luact', 'snapshot')
	local TESTDB_DIR = fs.abspath('tmp', 'luact', 'rocksdb', 'testdb3')

	fs.rmdir(SNAPSHOT_DIR)
	fs.rmdir(TESTDB_DIR)

	local sr = serde[serde.kind.serpent]
	local result_fsm = { a = 1, b = 1, c = 4, d = 2, e = 5, x = "9", y = 4, z = "5", w = "4"}


	---------------------------------------------------------------------
	-- normal functional test
	---------------------------------------------------------------------
	local x = (function ()
	print('--------------------- functional test ------------------------')
	local fsm = new_fsm()
	local store = rdbstore.new(TESTDB_DIR, 'state_test')
	local w = wal.new({hoge = 'fuga'}, store, sr, opts)
	local ss = snapshot.new(SNAPSHOT_DIR, sr)
	local st = state.new(fsm, w, ss, opts)
	local p = st.proposals
	local body = new_actor_body(p, st)
	local actor = luact(body)
	st.actor_body = body
	local n_quorum = 3
	function st:quorum()
		return n_quorum
	end

	st:add_replica_set(nil, replica_set)
	for _, addr_thread_id in ipairs(addr_thread_id_pairs) do
		local addr, thread_id = unpack(addr_thread_id)
		assert(st.replicators[addr], "address entry should be created")
		for k,v in pairs(st.replicators[addr]) do
			assert(k == thread_id, "thread entry should be created and this should be only entry in this addr")
		end
	end
	assert(st:last_index() == 1, "its first insert, so last index should be same as # of logs inserted")
	st:write_logs(nil, logs)
	assert(st:last_index() == 1 + #logs, "index should proceed according to # of logs inserted")

	n_quorum = 2
	st:remove_replica_set(nil, replica_set[4])
	st:write_logs(nil, logs2)
	assert(st:last_index() == 2 + #logs + #logs2, "index should proceed according to # of logs inserted")
	local last_index = tonumber(st:last_index())

	for i=1,1+#logs do
		local s = p.progress:at(i)
		-- print(i, s:valid(), s.quorum, s.current)
		assert(s:valid() and s.quorum == 3 and s.current == 1, "first entries should be valid and has 4 replicas, so quorum == 3")
	end
	for i=2+#logs,last_index do
		local s = p.progress:at(i)
		assert(s:valid() and s.quorum == 2 and s.current == 1, "second entries should be valid and has 3 replicas, so quorum == 2")
	end

	p:range_commit(actor, 1, last_index - 2)
	for i=1,1+#logs do
		local s = p.progress:at(i)
		-- print(i, s:valid(), s.quorum, s.current)
		assert(s:valid() and s.quorum == 3 and s.current == 2, "first entries should not satisfy quorum")
		assert(not body.check[i], "entry should not be committed")
	end
	for i=3+#logs, last_index-2 do
		local s = p.progress:at(i)
		assert(s:valid() and s.quorum == 2 and s:granted(), "second entries should satisfy quorum")
		assert(not body.check[i], "entry should not be committed")
	end
	for i=last_index-1,last_index do
		local s = p.progress:at(i)
		assert(s:valid() and s.quorum == 2 and s.current == 1, "last 2 entries should not satisfy quorum because not committed yet")
		assert(not body.check[i], "entry should not be committed")
	end

	p:range_commit(actor, 1, last_index - 2)
	clock.sleep(0.3)
	for i=1, last_index-2 do
		local s = p.progress:at(i)
		assert(not s, "second entries should satisfy quorum and be removed from progress list")
		assert(body.check[i], "entry should be committed")
	end
	local addr, thread_id = unpack(addr_thread_id_pairs[4])
	assert(not st.replicators[addr][thread_id], "removed thread should not remain in replica set")
	for i=last_index-1,last_index do
		local s = p.progress:at(i)
		assert(s:valid() and s.quorum == 2 and s.current == 1, "last 2 entries should not satisfy quorum because not committed yet")
		assert(not body.check[i], "entry should not be committed")
	end
	assert(util.table_equals(fsm, result_fsm), "log should be applied correctly")

	local idxlist = { 8, 12, 16 } -- 4 is removed by snapshot:trim()
	local count = 0
	for file in fs.opendir(SNAPSHOT_DIR):iter() do
		if file:match(snapshot.path_pattern) then
			count = count + 1
			-- print(count, idxlist[count], fs.path(SNAPSHOT_DIR, file), ffi.string(ss:path_of(idxlist[count])))
			assert(fs.path(SNAPSHOT_DIR, file) == ffi.string(ss:path_of(idxlist[count])), "snapshot should be created correclty")
		end
	end

	local remain_log_start_idx = tonumber(ss:last_index()) - opts.log_compaction_margin + 1
	-- w:dump()
	for i=1,remain_log_start_idx-1 do
		assert(not w:at(i), "logs should be removed by compaction")
		assert(not store:get_log(i, sr), "presisted logs should be remove by compaction")
	end
	for i=remain_log_start_idx,last_index do
		assert(w:at(i), "logs should not be removed by compaction")
		local log = store:get_log(i, sr)
		assert(log and log.index == i, "presisted logs should not be remove by compaction")
	end
	st:fin()
	return true
	end)()

	---------------------------------------------------------------------
	-- restore test (after normal functional test)
	---------------------------------------------------------------------
	local y = (function ()
	print('--------------------- restore test ------------------------')
	local fsm = new_fsm()
	local store = rdbstore.new(TESTDB_DIR, 'state_test')
	local w = wal.new({hoge = 'fuga'}, store, sr, opts)
	local ss = snapshot.new(SNAPSHOT_DIR, sr)
	local st = state.new(fsm, w, ss, opts)
	local body = new_actor_body(p, st)
	local actor = luact(body)
	local serpent = require 'serpent'

	assert(util.table_equals(fsm, result_fsm), "fsm should be restored correctly")
	assert(ss:last_index() == 16, "snapshot state should be restored correctly")
	assert(st.state.last_applied_idx == 16, "apply state should be restored correctly")

	-- -1 for unapplied WAL (index = 18)
	assert(#replica_set - 1 == #st.replica_set, "replica_set should be restored correctly")
	for i, id1 in ipairs({unpack(replica_set, 1, 3)}) do
		local found 
		for j, id2 in ipairs(st.replica_set) do
			-- print('st rep:', id2)
			if uuid.equals(id1, id2) then
				found = true
				break
			end
		end
		if not found then
			print(id1, 'not found')
		end
		assert(found, "replica_set should be restored correctly")
	end
	st:fin()
	return true
	end)()

end, function (e)
	logger.error('err', e)
	os.exit(-2)
end)

luact.stop()
end)

return true
