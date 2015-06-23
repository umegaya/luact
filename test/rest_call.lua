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

	local cnt = 0
	for key, val in pairs({ 
		hoge = "fuga",
		bar = "baz",
		piyo = "puya",
	}) do
		luact.tentacle(function (k, v)
			local resp = ref.GET('/response-headers?'..k..'='..v) 
			local status, headers = resp:payload()
			assert(headers[k] == v, "value for "..tostring(k).." should be "..tostring(v).." but "..tostring(headers[k]))
			resp:fin()
			cnt = cnt + 1
			if cnt >= 3 then
				luact.stop()
			end
		end, key, val)
	end
	return true
end)

return true