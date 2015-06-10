local luact = require 'luact.init'

luact.start({
	datadir = "/tmp/luact",
	n_core = 2, exclusive = true,
	dht = {
		gossip_port = false,	
	}, 
}, function ()
	local luact = require 'luact.init'
	local conn = require 'luact.conn'
	if luact.thread_id == 1 then
		luact.listen('tcp://0.0.0.0:8080')
		luact.register('/rest/api', function ()
			return {
				close_me = function (v)
					local p = luact.peer('/recver')
					p.you_are_about_to_die(v)
					luact.tentacle(function (c)
						luact.clock.sleep(0.1)
						luact.close_peer(c)
					end, p)
				end,
			}
		end)
	else
		luact.clock.sleep(0.1)
		local ref = luact.ref('tcp://127.0.0.1:8080/rest/api')
		local msg_ken4ro = "お前はもう死んでる........ "
		local msg_zedo = "なにィ〜〜〜！？"
		local msg
		luact.register('/recver', function ()
			return {
				you_are_about_to_die = function (v)
					msg = msg_ken4ro..v
					logger.info(msg)
				end,
			}
		end)
		ref.close_me(msg_zedo)
		luact.clock.sleep(0.2)
		assert(not conn.find_by_hostname('tcp://127.0.0.1:8080'))
		assert(msg == (msg_ken4ro..msg_zedo))
	end
end)

return true
