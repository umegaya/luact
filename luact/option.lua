return {
	cache_dir = "/tmp/luact",
	work_dir = "/tmp/luact", 
	n_core = 1, exclusive = true,
	bootstrap = false, 
	arbiter = {
		kind = "raft", 
		--[[ when raft
		config = {
			logsize_snapshot_threshold = 10000,
			initial_proposal_size = 1024,
			log_compaction_margin = 10240, 
			snapshot_file_preserve_num = 3, 
			election_timeout_sec = 0.15,
			heartbeat_timeout_sec = 1.0,
			proposal_timeout_sec = 5.0,
			serde = "serpent", -- serde using against storage.
			storage = "rocksdb",
			work_dir = luact.DEFAULT_ROOT_DIR,
		}, 
		]]
	},
	gossiper = {
		kind = "gossip",
	},
	conn = {
		internal_proto = "tcp+serpent",
		internal_port = "8008", 
		use_connection_cache = false,
	}, 
	actor = {
		-- startup_at = number,
		-- local_address = uint32_t,
	}, 
	router = {
		timeout_resolution = 1, -- 1 sec
	},
	dht = {

	}, 
}
