local msgidgen = require 'luact.msgid'
local dht = require 'luact.dht'
local conn = require 'luact.conn'
local uuid = require 'luact.uuid'
local actor = require 'luact.actor'
local clock = require 'luact.clock'

local tentacle = require 'pulpo.tentacle'
-- tentacle.DEBUG2 = true
local exception = require 'pulpo.exception'

local _M = {}
local debug_log
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
local timeout_periods = {}

local KIND = 1
local UUID = 2
local MSGID = 3
local METHOD = 4
local ARGS = 5

local RESP_MSGID = 2
local RESP_ARGS = 3

local NOTIFY_UUID = 2
local NOTIFY_METHOD = 3
local NOTIFY_ARGS = 4

local function dest_assinged_this_thread(msg)
	return uuid.owner_thread_of(msg[UUID])
end

function _M.internal(conn, message)
	-- logger.notice('router_internal', unpack(message))
	local k = message[KIND]
	local kind,notice = bit.band(k, EXCLUDE_NOTICE_MASK), bit.band(k, NOTICE_MASK) ~= 0
	if kind == KIND_RESPONSE then
		_M.respond(message)
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
					actor.dispatch_sys(msg[NOTIFY_UUID], msg[NOTIFY_METHOD], unpack(msg, NOTIFY_ARGS))
				end, message)
			elseif kind == KIND_CALL then
				tentacle(function (msg)
					actor.dispatch_call(msg[NOTIFY_UUID], msg[NOTIFY_METHOD], unpack(msg, NOTIFY_ARGS))
				end, message)
			elseif kind == KIND_SEND then
				tentacle(function (msg)
					actor.dispatch_send(msg[NOTIFY_UUID], msg[NOTIFY_METHOD], unpack(msg, NOTIFY_ARGS))
				end, message)
			end
		end
	elseif notice then
		tentacle(function (msg)
			local s = uuid.from_local_id(msg[NOTIFY_UUID])
			pcall(s[msg[NOTIFY_METHOD]], unpack(msg, NOTIFY_ARGS))
		end, message)
	else
		tentacle(function (sock, msg)
			local s = uuid.from_local_id(msg[UUID])
			sock:resp(msg[MSGID], pcall(s[msg[METHOD]], unpack(msg, ARGS)))
		end, conn, message)
	end	
	-- logger.notice('router exit', unpack(message))
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
		_M.respond(message)
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
				pcall(dht[msg[NOTIFY_UUID]][msg[NOTIFY_METHOD]], unpack(msg[NOTIFY_ARGS]))
			end, message)
		elseif kind == KIND_SEND then
			tentacle(function (msg)
				pcall(dht[msg[NOTIFY_UUID]][msg[NOTIFY_METHOD]], dht[msg[UUID]], unpack(msg[NOTIFY_ARGS]))
			end, message)
		end
	end
end

function _M.respond(message)
	local msgid = message[RESP_MSGID]
	timeout_periods[msgid] = nil
	co = coromap[msgid]
	if co then
		-- logger.info('respond', msgid, unpack(message, RESP_ARGS))
		tentacle.resume(co, unpack(message, RESP_ARGS))
		coromap[msgid] = nil
	end
end
function _M.respond_by_msgid(msgid, ...)
	timeout_periods[msgid] = nil
	co = coromap[msgid]
	if co then
		tentacle.resume(co, ...)
		coromap[msgid] = nil
	end
end

function _M.unregist(msgid)
	coromap[msgid] = nil
end

function _M.regist(co, timeout)
	local msgid = msgidgen.new()
	coromap[msgid] = co
	if tentacle.DEBUG2 then
		co.bt2 = debug.traceback()
	end
	if timeout then
		local nt = clock.get()
		timeout_periods[msgid] = (timeout + nt)
		debug_log('msgid=', msgid, 'settimeout', timeout_periods[msgid], timeout, nt)
	end
	return msgid
end

function _M.initialize(opts)
	if _M.DEBUG then
		debug_log = function (...)
			logger.notice(...)
		end
	else
		debug_log = function (...) end
	end
	-- TODO : prepare multiple timeout check queue for shorter timeout duration
	clock.timer(opts.timeout_resolution / 2, function ()
		local nt = clock.get()
		for id,tv in pairs(timeout_periods) do
			debug_log('nt:tv=', nt, tv)
			if nt > tv then
				debug_log('msgid=',id,'timeout')
				local co = coromap[id]
				if co then
					coromap[id] = nil
					if tentacle.DEBUG2 then
						tentacle.resume(co, nil, exception.new('actor_timeout', id, co.bt, co.bt2))
					else
						tentacle.resume(co, nil, exception.new('actor_timeout', id))
					end
				end
				timeout_periods[id] = nil
			end
		end
	end)
end

return _M
