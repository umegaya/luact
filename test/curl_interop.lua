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
	local ffi = require 'ffiex.init'
	local util = luact.util
	luact.listen('https://0.0.0.0:8443')
	luact.listen('http://0.0.0.0:8080')
	luact.listen('https+json://0.0.0.0:8444')
	luact.listen('http+json://0.0.0.0:8081')
	local json_serde = serde[serde.kind.json]
	local body_fixture = json_serde:unpack_from_string(io.open('./test/tools/push_body.json'):read('*a'))
	local payload_received 
	luact.register('/rest/api', function ()
		return {
			login = function (acc, pass)
				print('login called', acc, pass)
				return pass == 3
			end,
			push = function (verb, headers, body)
				assert(verb == "POST")
				assert(headers["User-Agent"] == "foo")
				local body_json = json_serde:unpack_from_string(body)
				assert(util.table_equals(body_fixture, body_json))
				payload_received = true
				return "ok"
			end,
		}
	end)
	local msgid = 111
	-- KIND, MSGID, CONTEXT, ARGS...
	local payload = {1, msgid, nil, "user", 3}
	local pbuf = require 'luact.pbuf'
	local fin_count = 0
	local function proc(proto, port, sr_kind, bin)
		local buf = luact.memory.alloc_typed('luact_rbuf_t')
		buf:init()
		local sr = serde[sr_kind]
		sr:pack(buf, payload)
		local cmd
		-- equivalent to actor.login("user", 3)
		if bin then
			cmd = ([[echo %s '%s' | curl -s -k -H 'User-Agent: Luact-RPC' --data-binary @- %s://127.0.0.1:%s/rest/api/login]]):format(
				ffi.os == "Linux" and "-e" or "", 
				luact.util.hex_escape(buf:curr_p(), buf:available()), proto, tostring(port)
			)
			if ffi.os == "Linux" then
				cmd = ('bash -c "%s"'):format(cmd)
			end
		else
			cmd = ([[curl -s -k -H "User-Agent: Luact-RPC" -d '%s' @- %s://127.0.0.1:%s/rest/api/login]]):format(
				ffi.string(buf:curr_p(), buf:available()), proto, tostring(port)
			)
		end
		local exitcode, out = luact.process.execute(cmd)
		buf:fin()
		buf:from_buffer(ffi.cast('char *', out), #out)
		local parsed = sr:unpack(buf)
		assert(parsed[2] == msgid and parsed[3] and parsed[4])
		fin_count = fin_count + 1
		print('proc finish', proto, port, fin_count)
		if fin_count >= 5 then
			print('graceful stop')
			luact.stop()
		end
	end

	luact.tentacle(proc, "https", 8443, serde.kind.msgpack, true)
	luact.tentacle(proc, "http", 8080, serde.kind.msgpack, true)
	luact.tentacle(proc, "https", 8444, serde.kind.json)
	luact.tentacle(proc, "http", 8081, serde.kind.json)
	-- [=[
	luact.tentacle(function ()
		local sr = serde[serde.kind.json]
		local buf = luact.memory.alloc_typed('luact_rbuf_t')
		local ec, out = luact.process.execute(
			[[curl -s -k -H 'User-Agent: foo' --data @./test/tools/push_body.json https://127.0.0.1:8444/rest/api/push]])
		assert(payload_received, "payload not received:"..out)
		buf:fin()
		buf:from_buffer(ffi.cast('char *', out), #out)
		local parsed = sr:unpack(buf)
		assert(parsed[1] == "ok")
		fin_count = fin_count + 1
		print('json request finish', fin_count)
		if fin_count >= 5 then
			print('graceful stop')
			luact.stop()
		end
	end)
	--]=]
	return true
end)

return true
