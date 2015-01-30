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
local KIND_MASK = NOTICE_MASK - 1

local KIND_SYS = 0
local KIND_CALL = 1
local KIND_SEND = 2
local KIND_RESPONSE = 3

local CONTEXT_PEER_ID = 1
local CONTEXT_TIMEOUT = 2
local CONTEXT_TXN_COORDINATOR = 3

_M.SYS = KIND_SYS
_M.CALL = KIND_CALL
_M.SEND = KIND_SEND
_M.RESPONSE = KIND_RESPONSE
_M.NOTICE_SYS = bit.bor(KIND_SYS, NOTICE_MASK)
_M.NOTICE_CALL = bit.bor(KIND_CALL, NOTICE_MASK)
_M.NOTICE_SEND = bit.bor(KIND_SEND, NOTICE_MASK)
_M.NOTICE_MASK = NOTICE_MASK

_M.CONTEXT_PEER_ID = CONTEXT_PEER_ID
_M.CONTEXT_TIMEOUT = CONTEXT_TIMEOUT
_M.CONTEXT_TXN_COORDINATOR = CONTEXT_TXN_COORDINATOR

local KIND = 1
local UUID = 2
local MSGID = 3
local METHOD = 4
local CONTEXT = 5
local ARGS = 6

local RESP_MSGID = 2
local RESP_ARGS = 3

local NOTIFY_UUID = 2
local NOTIFY_METHOD = 3
local NOTIFY_ARGS = 4

_M.KIND = KIND
_M.UUID = UUID
_M.MSGID = MSGID
_M.METHOD = METHOD
_M.CONTEXT = CONTEXT
_M.ARGS = ARGS

_M.NOTIFY_UUID = NOTIFY_UUID
_M.NOTIFY_METHOD = NOTIFY_METHOD
_M.NOTIFY_ARGS = NOTIFY_ARGS


-- variables
local coromap = {}
local timeout_periods = {}
local dht


-- tentacle runner
local function run_notice(dispatcher, msg)
	dispatcher(msg[NOTIFY_UUID], msg[NOTIFY_METHOD], unpack(msg, NOTIFY_ARGS))
end

local function run_request(c, dispatcher, msg)
	tentacle.set_context(msg[CONTEXT])
	c:resp(msg[MSGID], dispatcher(msg[UUID], msg[METHOD], unpack(msg, ARGS)))
end

function _M.internal(connection, message)
	-- logger.notice('router_internal', unpack(message))
	local k = message[KIND]
	local kind,notice = bit.band(k, KIND_MASK), bit.band(k, NOTICE_MASK) ~= 0
	if kind == KIND_RESPONSE then
		_M.respond(message)
	else
		local thread_id = uuid.thread_id_from_local_id(message[UUID])
		if thread_id == pulpo.thread_id then
			if notice then
				if kind == KIND_SYS then
					tentacle(run_notice, actor.dispatch_sys, message)
				elseif kind == KIND_CALL then
					tentacle(run_notice, actor.dispatch_call, message)
				elseif kind == KIND_SEND then
					tentacle(run_notice, actor.dispatch_send, message)
				end
			else
				if kind == KIND_SYS then
					tentacle(run_request, connection, actor.dispatch_sys, message)
				elseif kind == KIND_CALL then
					tentacle(run_request, connection, actor.dispatch_call, message)
				elseif kind == KIND_SEND then
					tentacle(run_request, connection, actor.dispatch_send, message)
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
local function vid_call_with_retry(id, cmd, method, ctx, ...)
::RETRY::
	local a = dht:get(id)
	if not a then return false, exception.new('actor_not_found', 'vid', id) end
	local c = conn.get(a)
	-- logger.notice('vid_call_with_retry', ...)
::RETRY2::
	local r = {c:send_and_wait(cmd, uuid.local_id(a), method, ctx, ...)}
	-- logger.report('vid_call_with_retry result', unpack(r))
	if r[1] then
		return unpack(r)
	elseif r[2]:is('actor_not_found') and (not retry) then
		retry = true
		dht:refresh(id)
		goto RETRY
	elseif r[2]:is('actor_temporary_fail') then
		clock.sleep(0.5)
		goto RETRY2
	else
		return unpack(r)
	end
end
local function vid_notify(id, cmd, method, ...)
	local a = dht:get(id)
	if not a then return false, exception.new('actor_not_found', 'vid', id) end
	local c = conn.get(a)
	c:rawsend(cmd, uuid.local_id(a), method, ...)
end

local context_work = {}
function _M.external(connection, message, from_untrusted)
	-- logger.notice('router_external', unpack(message))
	local k = message[KIND]
	local notice = bit.band(k, NOTICE_MASK) ~= 0
	if from_untrusted then
		if message[METHOD][1] == '_' then
			connection:resp(message[MSGID], false, exception.raise('invalid', 'protected call', method[METHOD]))
			return
		end
	end
	if k == KIND_RESPONSE then
		_M.respond(message)
		return
	end
	if notice then
		tentacle(function (c, cmd, msg)
			vid_notify(msg[NOTIFY_UUID], cmd, msg[NOTIFY_METHOD], unpack(msg, NOTIFY_ARGS))
		end, connection, k, message)	
	else
		tentacle(function (c, cmd, msg)
			local ctx = msg[CONTEXT] or context_work
			ctx[CONTEXT_PEER_ID] = c:peer_id()
			c:resp(msg[MSGID], vid_call_with_retry(msg[UUID], cmd, msg[METHOD], ctx, unpack(msg, ARGS)))
		end, connection, k, message)
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

function _M.regist(co, limit)
	local msgid = msgidgen.new()
	coromap[msgid] = co
	if limit then
		timeout_periods[msgid] = limit
		-- co.bt = debug.traceback()
		debug_log('msgid=', msgid, 'settimeout', timeout_periods[msgid])
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
					tentacle.resume(co, false, exception.new_with_bt('actor_timeout', co.bt or "", id))
				end
			end
		end
	end)
end

return _M
