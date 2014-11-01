local msgidgen = require 'luact.msgid'
local dht = require 'luact.dht'
local conn = require 'luact.conn'
local uuid = require 'luact.uuid'
local actor = require 'luact.actor'

local tentacle = require 'pulpo.tentacle'

local _M = {}
local NOTICE_BIT = 2
local NOTICE_MASK = bit.lshift(1, NOTICE_BIT)
local EXCLUDE_NOTICE_MASK = bit.lshift(1, NOTICE_BIT) - 1

local KIND_SYS = 0
local KIND_CALL = 1
local KIND_SEND = 2
local KIND_RESPONSE = 3

_M.SYS = KIND_SYS
_M.CALL = KIND_CALL
_M.SEND = KIND_SEND
_M.RESPONSE = KIND_RESPONSE
_M.NOTICE_SYS = bit.bor(KIND_SYS, NOTICE_MASK)
_M.NOTICE_CALL = bit.bor(KIND_CALL, NOTICE_MASK)
_M.NOTICE_SEND = bit.bor(KIND_SEND, NOTICE_MASK)

local coromap = {}

local KIND = 1
local UUID = 2
local MSGID = 3
local METHOD = 4
local ARGS = 5

local RESP_MSGID = 2
local RESP_ARGS = 3

local function dest_assinged_this_thread(msg)
	return uuid.owner_thread_of(msg[UUID])
end

function _M.internal(conn, message)
	logger.notice('router_internal', unpack(message))
	local k = message[KIND]
	local kind,notice = bit.band(k, EXCLUDE_NOTICE_MASK), bit.band(k, NOTICE_MASK) ~= 0
	if kind == KIND_RESPONSE then
		local msgid = message[RESP_MSGID]
		co = coromap[msgid]
		if co then
			coroutine.resume(co, unpack(message, RESP_ARGS))
			coromap[msgid] = nil
		end
	elseif dest_assinged_this_thread(message) then
		if not notice then
			if kind == KIND_SYS then
				tentacle(function (sock, msg)
					sock:resp(msg[MSGID], actor.dispatch_sys(msg[UUID], msg[METHOD], unpack(msg, ARGS)))
				end, conn, message)
			elseif kind == KIND_CALL then
				tentacle(function (sock, msg)
					sock:resp(msg[MSGID], actor.dispatch_call(msg[UUID], msg[METHOD], unpack(msg, ARGS)))
				end, conn, message)
			elseif kind == KIND_SEND then
				tentacle(function (sock, msg)
					sock:resp(msg[MSGID], actor.dispatch_send(msg[UUID], msg[METHOD], unpack(msg, ARGS)))
				end, conn, message)
			end
		else
			if kind == KIND_SYS then
				tentacle(function (msg)
					actor.dispatch_sys(msg[UUID], msg[METHOD], unpack(msg, ARGS))
				end, message)
			elseif kind == KIND_CALL then
				tentacle(function (msg)
					actor.dispatch_call(msg[UUID], msg[METHOD], unpack(msg, ARGS))
				end, message)
			elseif kind == KIND_SEND then
				tentacle(function (msg)
					actor.dispatch_send(msg[UUID], msg[METHOD], unpack(msg, ARGS))
				end, message)
			end
		end
	elseif notice then
		tentacle(function (msg)
			pcall(msg[UUID][msg[METHOD]], unpack(msg[ARGS]))
		end)
	else
		tentacle(function (sock, msg)
			sock:resp(msg[MSGID], pcall(msg[UUID][msg[METHOD]], unpack(msg[ARGS])))
		end, conn, message)
	end	
end

function _M.external(conn, message, from_untrusted)
	local k = message[KIND]
	local kind,notice = bit.band(k, EXCLUDE_NOTICE_MASK), bit.band(k, NOTICE_MASK) ~= 0
	if from_untrusted then
		if message[METHOD][1] == '_' then
			sock:resp(message[MSGID], false, "calling protected method from untrusted region")
			return
		end
	end
	if kind == KIND_RESPONSE then
		local msgid = message[RESP_MSGID]
		co = coromap[msgid]
		if co then
			coroutine.resume(co, unpack(message, RESP_ARGS))
			coromap[msgid] = nil
		end
	elseif not notice then
		if kind == KIND_CALL then
			tentacle(function (sock, msg)
				sock:resp(msg[MSGID], pcall(dht[msg[UUID]][msg[METHOD]], unpack(msg[ARGS])))
			end, conn, message)
		elseif kind == KIND_SEND then
			tentacle(function (sock, msg)
				sock:resp(msg[MSGID], pcall(dht[msg[UUID]][msg[METHOD]], dht[msg[UUID]], unpack(msg[ARGS])))
			end, conn, message)	
		end
	else
		if kind == KIND_CALL then
			tentacle(function (msg)
				pcall(dht[msg[UUID]][msg[METHOD]], unpack(msg[ARGS]))
			end, message)
		elseif kind == KIND_SEND then
			tentacle(function (msg)
				pcall(dht[msg[UUID]][msg[METHOD]], dht[msg[UUID]], unpack(msg[ARGS]))
			end, message)
		end
	end
end

function _M.regist(co, timeout)
	local msgid = msgidgen.new()
	coromap[msgid] = co
	return msgid
end

return _M
