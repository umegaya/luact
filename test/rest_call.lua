local luact = require 'luact.init'

luact.start({
	datadir = "/tmp/luact",
	n_core = 1, exclusive = true,
	dht = {
		gossip_port = false,	
	}, 
}, function ()
	local luact = require 'luact.init'
	local serde = require 'luact.serde'
	local json = serde[serde.kind.json]
	local ref = luact.ref('http+json://httpbin.org:80')
	local resp, status, headers, b, blen 
	local payload = {
		["great-payload"] = "great!!!"
	}
	
	resp = ref.POST('/post', payload)
	status, headers, b, blen = resp:payload()
	assert(luact.util.table_equals(json:unpack_from_string(b, blen).json, payload))
	resp:fin()

	ok, resp = pcall(ref.GET, '/status/418')
	assert((not ok) and resp:is('http') and resp.args[1] == 'status' and resp.args[2] == 418)

	resp = ref.GET('/response-headers?key=val') 
	status, headers = resp:payload()
	assert(headers['key'] == 'val')
	resp:fin()
end)

return true