local luact = require 'luact.init'

luact.start({
	datadir = "/tmp/luact",
	n_core = 1, exclusive = true,
	dht = {
		gossip_port = false,	
	}, 
}, function ()
	local luact = require 'luact.init'
	local ref = luact.ref('https+json://great.webservice.com/api/v1.1')
	local resp = ref.POST('/login', { ["X-Great-Signature"] = "xxxxxxxxxxxxxx" }, {
		["great-payload"] = "great!!!"
	})
	local status, headers, b, blen = resp:payload()
	resp:fin()
end)