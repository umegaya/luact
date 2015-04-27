local luact = require 'luact.init'

luact.start({
	datadir = "/tmp/luact",
	n_core = 1, exclusive = true,
	dht = {
		gossip_port = false,	
	}, 
}, function ()
	local luact = require 'luact.init'
	luact.listen('https://0.0.0.0:8443')
	luact.listen('http://0.0.0.0:8080')
	luact.register('/rest/api', function ()
		return {
			login = function (acc, pass)
				print('login called', acc, pass)
				return pass == 3
			end,
		}
	end)
	local msgid = 111
	local payload = {1, msgid, "user", nil, 3}
	local serde = require 'luact.serde'
	local pbuf = require 'luact.pbuf'
	local fin_count = 0
	local function proc(proto, port, sr_kind)
		local buf = luact.memory.alloc_typed('luact_rbuf_t')
		buf:init()
		local sr = serde[sr_kind]
		sr:pack(buf, payload)
		local cmd = ([[echo '%s' | curl -s -k --data-binary @- %s://127.0.0.1:%s/rest/api/login]]):format(
			luact.util.hex_escape(buf:curr_p(), buf:available()), proto, tostring(port)
		)
		local exitcode, out = luact.process.execute(cmd)
		buf:fin()
		buf:from_buffer(ffi.cast('char *', out), #out)
		local parsed = sr:unpack(buf)
		assert(parsed[2] == msgid and parsed[3] and parsed[4])
		fin_count = fin_count + 1
		print('proc finish', proto, port, fin_count)
		if fin_count >= 2 then
			print('graceful stop')
			luact.stop()
		end
	end

	luact.tentacle(proc, "https", 8443, serde.kind.msgpack)
	luact.tentacle(proc, "http", 8080, serde.kind.msgpack)
	return true
end)

return true
