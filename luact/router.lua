local msgidgen = require 'luact.msgid'
local conn = require 'luact.conn'
local uuid = require 'luact.uuid'
local actor = require 'luact.actor'
local clock = require 'luact.clock'
local vid = require 'luact.vid'

local pulpo = require 'pulpo.init'
local tentacle = require 'pulpo.tentacle'
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
local dht

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

function _M.internal(connection, message)
	-- logger.notice('router_internal', unpack(message))
	local k = message[KIND]
	local kind,notice = bit.band(k, EXCLUDE_NOTICE_MASK), bit.band(k, NOTICE_MASK) ~= 0
	if kind == KIND_RESPONSE then
		_M.respond(message)
	else
		local thread_id = uuid.thread_id_from_local_id(message[UUID])
		if thread_id == pulpo.thread_id then
			if not notice then
				if kind == KIND_SYS then
					tentacle(function (c, msg)
						c:resp(msg[MSGID], actor.dispatch_sys(msg[UUID], msg[METHOD], unpack(msg, ARGS)))
					end, connection, message)
				elseif kind == KIND_CALL then
					tentacle(function (c, msg)
						c:resp(msg[MSGID], actor.dispatch_call(msg[UUID], msg[METHOD], unpack(msg, ARGS)))
					end, connection, message)
				elseif kind == KIND_SEND then
					tentacle(function (c, msg)
						c:resp(msg[MSGID], actor.dispatch_send(msg[UUID], msg[METHOD], unpack(msg, ARGS)))
					end, connection, message)
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
		else
			local dest_conn = conn.get_by_thread_id(thread_id)
			if notice then
				dest_conn:rawsend(unpack(message))
			else
				tentacle(function (c, dc, msg)
					local resp_msgid = msg[MSGID]
					local msgid = _M.regist(tentacle.running())
					msg[MSGID] = msgid
					dc:rawsend(unpack(msg))
					c:resp(resp_msgid, tentacle.yield(msgid))
				end, connection, dest_conn, message)				
			end
		end	
	end
	-- logger.notice('router exit', unpack(message))
end

-- maintain availability during failover on going.
local function vid_call_with_retry(id, sent, method, ...)
::RETRY::
	local a = dht:get(id)
	local r, retry
	if sent then
		r = {pcall(a[method], a, ...)}
	else
		r = {pcall(a[method], ...)}
	end
	if r[1] then
		return unpack(r)
	elseif r[2]:is('actor_not_found') and (not retry) then
		retry = true
		dht:reflesh(id)
		goto RETRY
	elseif r[2]:is('actor_temporary_failure') then
		clock.sleep(0.5)
		goto RETRY
	else
		return unpack(r)
	end
end

function _M.external(connection, message, from_untrusted)
	logger.notice('router_external', unpack(message))
	local k = message[KIND]
	local kind,notice = bit.band(k, EXCLUDE_NOTICE_MASK), bit.band(k, NOTICE_MASK) ~= 0
	if from_untrusted then
		if message[METHOD][1] == '_' then
			connection:resp(message[MSGID], false, exception.raise('invalid', 'protected call', method[METHOD]))
			return
		end
	end
	if kind == KIND_RESPONSE then
		_M.respond(message)
	elseif not notice then
		if kind == KIND_CALL then
			tentacle(function (c, msg)
				c:resp(msg[MSGID], vid_call_with_retry(msg[UUID], false, msg[METHOD], unpack(msg, ARGS)))
			end, connection, message)
		elseif kind == KIND_SEND then
			tentacle(function (c, msg)
				c:resp(msg[MSGID], vid_call_with_retry(msg[UUID], true, msg[METHOD], unpack(msg, ARGS)))
			end, connection, message)	
		end
	else
		if kind == KIND_CALL then
			tentacle(function (msg)
				vid_call_with_retry(msg[UUID], false, msg[METHOD], unpack(msg, ARGS))
			end, message)
		elseif kind == KIND_SEND then
			tentacle(function (msg)
				vid_call_with_retry(msg[UUID], true, msg[METHOD], unpack(msg, ARGS))
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
	timeout_periods[id] = nil
end

function _M.regist(co, timeout)
	local msgid = msgidgen.new()
	coromap[msgid] = co
	if timeout then
		local nt = clock.get()
		timeout_periods[msgid] = (timeout + nt)
		debug_log('msgid=', msgid, 'settimeout', timeout_periods[msgid], timeout, nt)
	end
	return msgid
end

function _M.initialize(opts)
	dht = vid.dht
	if _M.DEBUG then
		debug_log = function (...)
			logger.warn(...)
		end
	else
		debug_log = function (...) end
	end
	-- TODO : prepare multiple timeout check queue for shorter timeout duration
	clock.timer(opts.timeout_resolution / 2, function ()
		local nt = clock.get()
		for id,tv in pairs(timeout_periods) do
			if nt > tv then
				debug_log('msgid=',id,'timeout')
				timeout_periods[id] = nil
				local co = coromap[id]
				if co then
					coromap[id] = nil
					tentacle.resume(co, false, exception.new_with_bt('actor_timeout', "", id))
				end
			end
		end
	end)
end

return _M
