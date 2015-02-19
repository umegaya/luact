return {
	logdir = false, -- "/tmp/luact/logs",
	datadir = "/tmp/luact", 
	n_core = 1, exclusive = true,
	startup_at = nil,
	parent_address = nil,
	local_address = nil,
	arbiter = {
		kind = "raft", 
		config = {}, -- see cluster/raft.lua for other keys and defaults
	},
	gossiper = {
		kind = "gossip",
		config = {
			nodelist = {}, 
		}, -- see cluster/gossip.lua for other keys and defaults
	},
	conn = {
		internal_proto = "tcp+msgpack",
		internal_port = 8008, 
		use_connection_cache = false,
	}, 
	actor = {
	}, 
	router = {
		timeout_resolution = 1, -- 1 sec
	},
	dht = {
	}, 
	vid = {

	}, 
}
