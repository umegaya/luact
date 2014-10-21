local msgidgen = require 'luact.msgid'
local conn -- luact.conn
local dht = require 'luact.dht'

local _M = {}
_M.CALL = 1
_M.SEND = 2
_M.NOTICE_CALL = 3
_M.NOTICE_SEND = 4
_M.RESPONSE = 5

local coromap = {}
local local_thread_connections = setmetatable({}, {
	__index = function (t, id)
		local conn = conn.get_by_thread_id(id)
		rawset(t, id, conn)
		return conn
	end,
})

local IDX_KIND = 1
local IDX_UUID = 2
local IDX_MSGID = 3
local IDX_METHOD = 4
local IDX_ARGS = 5

function _M.internal(conn, message)
	local kind = message[IDX_KIND]
	if kind == CALL then
		if dest_assinged_this_thread(message) then
			tentacle(function (sock, msg)
				sock:resp(msg[IDX_MSGID], actor.dispatch_call(msg))
			end, conn, message)
		else
			tentacle(function (sock, msg)
				sock:resp(msg[IDX_MSGID], pcall(msg[IDX_UUID][msg[IIDX_METHOD]], unpack(msg[IDX_ARGS])))
			end, conn, message)
		end
	elseif kind == SEND then
		if dest_assinged_this_thread(message) then
			tentacle(function (sock, msg)
				sock:resp(msg[IDX_MSGID], actor.dispatch_send(msg))
			end, conn, message)
		else
			tentacle(function (sock, msg)
				sock:resp(msg[IDX_MSGID], pcall(msg[IDX_UUID][msg[IIDX_METHOD]], msg[IDX_UUID], unpack(msg[IDX_ARGS])))
			end, conn, message)
		end
	elseif kind == NOTICE_CALL then
		if dest_assinged_this_thread(message) then
			tentacle(function (msg)
				actor.dispatch_call(msg)
			end, message)
		else
			tentacle(function (msg)
				pcall(msg[IDX_UUID][msg[IIDX_METHOD]], unpack(msg[IDX_ARGS]))
			end)
		end
	elseif kind == NOTICE_SEND then
		if dest_assinged_this_thread(message) then
			tentacle(function (msg)
				actor.dispatch_send(msg)
			end, message)
		else
			tentacle(function (sock, msg)
				pcall(msg[IDX_UUID][msg[IIDX_METHOD]], msg[IDX_UUID], unpack(msg[IDX_ARGS]))
			end)
		end
	elseif kind == RESPONSE then
		local msgid = parsed:msgid()
		co = coromap[msgid]
		if co then
			coroutine.resume(co, unpack(msg[IDX_ARGS]))
			coromap[msgid] = nil
		end
	end	
end

function _M.external(conn, message, from_untrusted)
	local kind = parsed[IDX_KIND]
	if from_untrusted then
		if parsed[IDX_METHOD][1] == '_' then
			sock:resp(message[IDX_MSGID], false, "calling protected method from untrusted region")
			return
		end
	end
	if kind == _M.CALL or kind == _M.SEND then
		pulpo.tentacle(function (sock, msg)
			sock:resp(msg[IDX_MSGID], 
				pcall(dht[msg[IDX_DEFS.UUID]][msg[IDX_METHOD]], unpack(msg[IDX_ARGS]))
			)
		end, conn, message)
	elseif kind == _M.NOTICE then
		pulpo.tentacle(function (msg)
			pcall(dht[msg[IDX_UUID]][msg[IDX_METHOD]], unpack(msg[IDX_ARGS]))
		end, message)
	elseif kind == _M.RESPONSE then
		local msgid = parsed:msgid()
		co = coromap[msgid]
		if co then
			coroutine.resume(co, unpack(parsed[IDX_ARGS]))
			coromap[msgid] = nil
		end
	end
end

function _M.regist(co)
	local msgid = msgidgen.new()
	coromap[msgid] = co
	return msgid
end

function _M.initialize(cmdl_arg)
	conn = require 'luact.conn'
end

return _M
