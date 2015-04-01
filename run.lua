local luact = require 'luact'

luact.start {
	exclusive = true, parse_arg = true,
	dht = { gossip_port = false }, 
}
