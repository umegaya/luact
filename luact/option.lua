return {
	cache_dir = "/tmp/luact",
	work_dir = "/tmp/luact", 
	n_core = 1, exclusive = true,
	bootstrap = false, 
	arbiter = {
		kind = "raft", 
		config = {}, -- see cluster/raft.lua for key and default
	},
	gossiper = {
		kind = "gossip",
		config = {}, -- see cluster/gossip.lua for key and default
	},
	conn = {
		internal_proto = "tcp+serpent",
		internal_port = 8008, 
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
