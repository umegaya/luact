local luact = require 'luact'

luact.start {
	exclusive = true,
	dht = { gossip_port = false }, 
}
