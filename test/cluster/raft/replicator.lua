local luact = require 'luact.init'

luact.start({
	cache_dir = "/tmp/luact",
	n_core = 1, exclusive = true,
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
		workdir = luact.DEFAULT_ROOT_DIR,
		
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
	local TESTDB_DIR = fs.abspath('tmp', 'luact', 'rocksdb', 'testdb4')
	fs.rmdir(SNAPSHOT_DIR)
	fs.rmdir(TESTDB_DIR)

	local FIRST_LOGSIZE = 20
	local function new_actor_body()
		return setmetatable({}, {
			__index = {
				append_entries = function (term, leader, prev_log_idx, prev_log_term, entries, leader_commit_idx)
				end,
				install_snapshot = function (term, leader, last_log_idx, last_log_term, nodes, size)
				end,
			}
		})
	end
	local body = new_actor_body()
	local actor = luact(body)
	local x

	-- test send entry
	x = (function ()
	local fsm = new_fsm()
	local store = rdbstore.new(TESTDB_DIR, 'state_test')
	local w = wal.new({hoge = 'fuga'}, store, sr, opts)
	local ss = snapshot.new(SNAPSHOT_DIR, sr)
	local st = state.new(fsm, w, ss, opts)
	local p = st.proposals
	function body:append_entries(term, leader, prev_log_idx, prev_log_term, entries, leader_commit_idx)
		if not entries then
			print('heartbeat:', term, leader)
			return term, true
		end
		print('append entries', term, prev_log_idx, prev_log_term)
		assert(term == 0 and prev_log_idx == 0 and prev_log_term == 0, "its first append entries, so log index and term is initial")
		assert(uuid.equals(leader, opts.debug_leader_id), "leader id should be same as specified")
		assert(#entries == FIRST_LOGSIZE, "correct entries should be sent")
		for i=1,FIRST_LOGSIZE do
			assert(entries[i].log.value == i, "correct entries should be sent")
		end
		return term, true, entries[FIRST_LOGSIZE].index
	end

	local rep = replicator.new(actor, st)
	local logs = {}
	for i=1,FIRST_LOGSIZE do
		table.insert(logs, { value = i })
	end
	st:write_logs(nil, logs)
	clock.sleep(0.5)

	end)()

end, function (e)
	logger.error('err', e)
end)

luact.stop()
end)

return true
