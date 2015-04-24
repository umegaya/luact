local luact = require 'luact.init'

luact.start({
	datadir = "/tmp/luact",
	n_core = 4, exclusive = true,
	dht = {
		gossip_port = false,	
	}, 
}, function ()
	local luact = require 'luact.init'
	luact.listen('http://0.0.0.0:8080')
	if luact.thread_id == 1 then
		luact.register('/rest/api', function ()
			return {
				login = function (acc, pass)
					return pass == 3
				end,
			}
		end)
	else
		luact.clock.sleep(0.1)
		local ref = luact.ref('http://127.0.0.1:8080/rest/api')
		local r = ref.login("user", luact.thread_id)
		logger.info(r, (luact.thread_id == 3))
		assert(r == (luact.thread_id == 3))
	end
end)

return true