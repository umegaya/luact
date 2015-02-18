local luact = require 'luact.init'

luact.start({
	datadir = "/tmp/luact",
	n_core = 1, exclusive = true,
	dht = {
		gossip_port = false, -- disable dht. vid will run in local mode
	}, 
}, function ()
local luact = require 'luact.init'
local ok,r = xpcall(function ()

	local state = require 'luact.cluster.raft.state'
	local wal = require 'luact.cluster.raft.wal'
	local snapshot = require 'luact.cluster.raft.snapshot'
	local replicator = require 'luact.cluster.raft.replicator'
	local rdbstore = require 'luact.cluster.store.rocksdb'
	local serde = require 'luact.serde'
	local sr = serde[serde.kind.serpent]
	local uuid = require 'luact.uuid'
	local clock = require 'luact.clock'
	local event = require 'pulpo.event'
	local util = require 'pulpo.util'
	local fs = require 'pulpo.fs'
	local memory = require 'pulpo.memory'
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
		datadir = luact.DEFAULT_ROOT_DIR,
		
		debug_node_kind = "leader", 
		debug_leader_id = uuid.debug_create_id("1.1.1.1", 1),
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
	local SNAPSHOT_DIR = fs.abspath('tmp', 'luact', 'snapshot2')
	local SNAPSHOT_DIR2 = fs.abspath('tmp', 'luact', 'snapshot3')
	local TESTDB_DIR = fs.abspath('tmp', 'luact', 'rocksdb', 'testdb4')

	local FIRST_LOGSIZE = 20
	local function new_actor_body()
		return setmetatable({}, {
			__index = {
				append_entries = function (term, leader, leader_commit_idx, prev_log_idx, prev_log_term, entries)
				end,
				install_snapshot = function (term, leader, fd)
				end,
			}
		})
	end
	local body, body2 = new_actor_body(), new_actor_body()
	local actor, actor2 = luact(body), luact(body2)
	local x

	-- test send entry
	print('----------------------- normal replication test ----------------------')
	x = (function ()
	fs.rmdir(SNAPSHOT_DIR)
	fs.rmdir(TESTDB_DIR)
	local fsm = new_fsm()
	local store = rdbstore.new(TESTDB_DIR, 'state_test')
	local w = wal.new({hoge = 'fuga'}, store, sr, opts)
	local ss = snapshot.new(SNAPSHOT_DIR, sr)
	local st = state.new(fsm, w, ss, opts)
	local p = st.proposals
	function st:quorum()
		return 3
	end
	function body:append_entries(term, leader, leader_commit_idx, prev_log_idx, prev_log_term, entries)
		if not entries then
			print('heartbeat:', term, leader)
			return term, true, 0
		end
		print('append entries', term, prev_log_idx, prev_log_term, #entries)
		assert(term == 0 and prev_log_idx == 0 and prev_log_term == 0, "its first append entries, so log index and term is initial")
		assert(uuid.equals(leader, opts.debug_leader_id), "leader id should be same as specified")
		if #entries > 0 then
			assert(#entries == FIRST_LOGSIZE, "correct entries should be sent")
			for i=1,FIRST_LOGSIZE do
				assert(entries[i].log.value == i, "correct entries should be sent")
			end
		end
		return term, true, 0
	end

	local rep, endev = replicator.new(actor, actor, st, true)
	local logs = {}
	for i=1,FIRST_LOGSIZE do
		table.insert(logs, { value = i })
	end
	st:write_logs(nil, logs)
	clock.sleep(0.5)

	for i=1,FIRST_LOGSIZE do
		local s = p.progress:at(i)
		-- write_logs' 1 + replicator commit's 1 == 2
		assert(s:valid() and s.quorum == 3 and s.current == 2, "proposal should be committed")
	end

	st.ev_log:emit('stop')
	rep:fin()
	st:fin()
	end)()

	-- failure
	print('----------------------- failure and install snapshot test ----------------------')
	x = (function ()
	fs.rmdir(SNAPSHOT_DIR)
	fs.rmdir(SNAPSHOT_DIR2)
	fs.rmdir(TESTDB_DIR)
	local fsm = new_fsm()
	local fsm2 = new_fsm()
	local store = rdbstore.new(TESTDB_DIR, 'state_test')
	local w = wal.new({hoge = 'fuga'}, store, sr, opts)
	local ss = snapshot.new(SNAPSHOT_DIR, sr)
	local ss2 = snapshot.new(SNAPSHOT_DIR2, sr)
	local st = state.new(fsm, w, ss, opts)
	local p = st.proposals
	local QUORUM = 3
	function st:quorum()
		return QUORUM
	end
	function st:append_param_for(st)
		return nil -- force append_entries to fail
	end
	function body:accepted()
		for idx, log in ipairs(p.accepted) do
			st:apply(log)
			p.accepted[idx] = nil
		end
	end
	function body:append_entries(term, leader, leader_commit_idx, prev_log_idx, prev_log_term, entries)
		if not entries then
			print('heartbeat:', term, leader)
			return term, true
		end
		print('append entries', term, prev_log_idx, prev_log_term)
		assert(#entries == FIRST_LOGSIZE, "correct entries should be sent")
		for i=1,FIRST_LOGSIZE do
			assert(entries[i].log[i] == i * 2, "correct entries should be sent")
		end
		return term, true, entries[FIRST_LOGSIZE].index
	end
	function body2:install_snapshot(term, leader, last_snapshot_index, fd)
		assert(term == 0, "term should be correct")
		assert(uuid.equals(leader, opts.debug_leader_id), "leader should be correct")
		local ok, rb = pcall(ss2.copy, ss2, fd, last_snapshot_index) 
		assert(ok, "store snapshot on local storage should succeed:"..tostring(rb))
		local idx,hd = ss2:restore(fsm2, rb)
		assert(hd, "header should be received correctly")
		assert(hd.index == 8 and hd.term == 0 and hd.n_replica == 0, "header should be received correctly")
		return true
	end


	local logs = {}
	for i=1,FIRST_LOGSIZE do
		table.insert(logs, { i, i * 2 })
	end
	st:write_logs(nil, logs)
	-- commit half of inserted logs
	for i=1,QUORUM do
		p:range_commit(actor, 1, math.ceil(FIRST_LOGSIZE/2))
	end
	clock.sleep(0.5)
	-- start replication
	print('start repl')
	local rep, endev = replicator.new(actor, actor2, st, true)	
	clock.sleep(1.0) -- wait for replicator done its work

	-- after meantime of sleep, fsm should updated (by snapshot)
	for i=1,8 do
		assert(fsm2[i] == i * 2, "snapshot data should apply correctly:"..tostring(i).."|"..tostring(fsm2[i]))
	end
	for i=9, FIRST_LOGSIZE do
		assert(not fsm2[i], "snapshot data should apply correctly:"..tostring(i).."|"..tostring(fsm2[i]))
	end
	print('success')

	st.ev_log:emit('stop')
	rep:fin()
	st:fin()
	end)()

end, function (e)
	logger.error('err', e)
	os.exit(-2)
end)

luact.stop()
end)

return true
