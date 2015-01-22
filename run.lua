local luact = require 'luact'

luact.start({
	cache_dir = "/tmp/lunarsurface",
	exclusive = true,
}, arg[1])
