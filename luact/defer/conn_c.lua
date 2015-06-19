local ffi = require 'ffiex.init'

local pulpo = require 'pulpo.init'

local pbuf = require 'luact.pbuf'
local read, write = pbuf.read, pbuf.write
local serde = require 'luact.serde'
-- serde.DEBUG = true
local uuid = require 'luact.uuid'
local vid = require 'luact.vid'
local peer = require 'luact.peer'
local clock = require 'luact.clock'
local actor = require 'luact.actor'
local msgidgen = require 'luact.msgid'
local supervise = require 'luact.supervise'
local router = require 'luact.router'
local future = require 'luact.future'

local thread = require 'pulpo.thread'
local tentacle = require 'pulpo.tentacle'
local exception = require 'pulpo.exception'
local memory = require 'pulpo.memory'
local gen = require 'pulpo.generics'
local socket = require 'pulpo.socket'
local event = require 'pulpo.event'
local util = require 'pulpo.util'
local linda = pulpo.evloop.io.linda

local _M = (require 'pulpo.package').module('luact.defer.conn_c')

ffi.cdef [[
	typedef struct luact_conn {
		pulpo_io_t *io;
		luact_rbuf_t rb;
		luact_wbuf_t wb;
		unsigned char serde_id, dead, http, padd;
		unsigned short task_processor_id, padd;
		uint32_t local_peer_id, numeric_ipv4;
	} luact_conn_t;
	typedef struct luact_ext_conn {
		pulpo_io_t *io;
		luact_rbuf_t rb;
		luact_wbuf_t wb;
		unsigned char serde_id, dead, http, padd;
		unsigned short task_processor_id, padd;
		uint32_t local_peer_id;
		char *hostname;
	} luact_ext_conn_t;
	typedef struct luact_local_conn {
		pulpo_pipe_io_t *mine, *yours;
		luact_rbuf_t rb;
		luact_wbuf_t wb;
		unsigned char serde_id, dead;
		unsigned short task_processor_id;
		int thread_id;
	} luact_local_conn_t;
]]
ffi.cdef (([[
typedef union luact_peer {
	uint64_t peer_id;
	struct luact_peer_format {
		uint32_t local_peer_id:%u;
		uint32_t thread_id:%u;
		uint32_t machine_id:32;
	} detail;
} luact_peer_t;
]]):format(32 - uuid.THREAD_BIT_SIZE, uuid.THREAD_BIT_SIZE))
_M.MAX_LOCAL_PEER_ID = bit.lshift(1, 32 - uuid.THREAD_BIT_SIZE) - 1


local AF_INET = ffi.defs.AF_INET
local AF_INET6 = ffi.defs.AF_INET6


--[[
 	connection managers (just hash map)
--]]
local cmap, lcmap = {}, {}
local peer_cmap = {}
local conn_free_list = {}
_M.local_cmap = lcmap -- for external use


-- periodic task
local conn_tasks_index = {}
local conn_tasks_mt = {
	__index = conn_tasks_index,
}
function conn_tasks_mt.new(per_processor, interval)
	return setmetatable({
		[1] = {id = 1},
		alive = true, map = {}, 
		per_processor = per_processor or 256,
		interval = interval or 3.0, 
	}, conn_tasks_mt)
end
function conn_tasks_index:add(conn)
	local list = self[1]
	if #list > self.per_processor then
		table.sort(self, function (a, b) return #a < #b end)
		list = self[1]
		if #list > self.per_processor then
			list = {}
			list.id = (#self + 1)
			self.map[list.id] = list
			table.insert(self, list)
			table.insert(list, conn)
			conn.task_processor_id = list.id
			clock.timer(self.interval, self, list)
			return
		end
	end
	conn.task_processor_id = list.id
	table.insert(list, conn)
end
function conn_tasks_index:remove(conn)
	if conn.task_processor_id <= 0 then
		return
	end
	local list = self.map[conn.task_processor_id]
	if not list then
		return 
	end
	for i=1,#list do
		local c = list[i]
		if c == conn then
			table.remove(list, i)
			return
		end
	end
end
function conn_tasks_index:process(conn_list)
	while self.alive do
		clock.sleep(self.interval)
		for i=1,#conn_list,1 do
			conn_list[i]:task()
		end
	end
end
local conn_tasks = conn_tasks_mt.new()


-- known machine stats
_M.stats = {}

-- remote conn metatable
local conn_index  = {}
local conn_mt = {
	__index = conn_index,
}
local function open_io(hostname, opts)
	local proto, sr, address, user, credential = _M.parse_hostname(hostname)
	-- print('open_io', proto, sr, address, debug.traceback())
	-- TODO : if user and credential is specified, how should we handle these?
	local p = pulpo.evloop.io[proto]
	assert(p.connect, exception.new('not_found', 'method', 'connect', proto))
	local ok, io = pcall(p.connect, address, opts)
	if not ok then
		logger.report('connect error', io)
		exception.raise(io)
	end
	return io, serde.kind[sr], proto:match('^http') and 1 or 0
end
function conn_index:init_buffer()
	self.rb:init()
	self.wb:init()
end
function conn_index:start_io(opts, sr, server)
	-- print('start_io', self, self.http, server, opts.internal, debug.traceback())
	local rev, wev
	if self.http ~= 0 then
		rev = tentacle(self.read_webext, self, self.io, true, sr)
		tentacle(self.sweeper, self, rev)
	else	
		wev = tentacle(self.write, self, self.io)
		if opts.internal then
			rev = tentacle(self.read_int, self, self.io, sr)
		elseif opts.trusted then
			rev = tentacle(self.read_ext, self, self.io, false, sr)
		else
			rev = tentacle(self.read_ext, self, self.io, true, sr)
		end
		tentacle(self.sweeper, self, rev, wev)
	end
	-- for example, normal http (not 2.0) is volatile.
	if not (server or opts.volatile_connection) then
		conn_tasks:add(self) -- start keeping alive
	else
		self.task_processor_id = 0 -- no task
	end
end
-- TODO : now tentacle become cancelable, so remove sweeper is possible.
function conn_index:sweeper(rev, wev)
	local tp,obj = event.wait(nil, rev, wev)
	-- assures coroutines are never execute any line
	if wev and obj == rev then
		tentacle.cancel(wev)
	elseif rev and obj == web then
		tentacle.cancel(rev)
	end
	-- these 2 line assures another tentacle (read for write/write for read)
	-- start to finish.
	self:close()
	self:destroy('error')
end
function conn_index:task()
	-- do keep alive
	local mid = self:machine_id()
	local ra = actor.root_of(mid, 1)
	-- _M.stats[mid] = ra:stat() -- used as heartbeat message
	-- TODO : check which thread accept this connection and use corresponding root actor.
	-- change second argument of actor.root_of
end
function conn_index:new(machine_ipv4, opts)
	if machine_ipv4 == 0 then
		exception.raise('invalid', 'machine_id', 0)
	end
	local hostname = _M.internal_hostname_by_addr(machine_ipv4)
	self.io,self.serde_id,self.http = open_io(hostname, opts)
	self.dead = 0
	self.numeric_ipv4 = machine_ipv4
	self.local_peer_id = 0
	self:start_io(opts, self:serde())
	return self
end
function conn_index:new_server(io, opts)
	self.io = io
	self.serde_id = serde.kind[opts.serde or _M.DEFAULT_SERDE]
	self.http = opts.http and 1 or 0
	self.dead = 0
	self.numeric_ipv4 = io:address():as_machine_id()
	self:assign_local_peer_id()
	self:start_io(opts, self:serde(), true)
	return self
end
function conn_index:serde()
	return serde[tonumber(self.serde_id)]
end
function conn_index:close()
	-- _M.stats[self:machine_id()] = nil
	conn_tasks:remove(self)
	self.dead = 1
	self.io:close()
end
local function conn_common_destroy(self, reason, map, free_list)
	if map[self:cmapkey()] and (map[self:cmapkey()] ~= self) then
		logger.fatal("connection not match:"..tostring(self).." and "..tostring(map[self:cmapkey()]).." "..tostring(self:cmapkey()))
	end
	if not _M.use_connection_cache then
		map[self:cmapkey()] = nil
		self.rb:fin()
		self.wb:fin()
		memory.free(self)
	logger.notice('conn free:', self)
	else
		-- TODO : cache conn object (to reduce malloc cost)
		map[self:cmapkey()] = nil
		self.rb:reset()
		self.wb:reset()
		table.insert(free_list, self)
	logger.notice('conn free(cache):', self)
	end
end
-- for server push
local peer_id_seed = 1
function conn_index:assign_local_peer_id()
	local start = peer_id_seed
	while peer_cmap[peer_id_seed] do
		peer_id_seed = peer_id_seed + 1
		if (peer_id_seed - start) > 100000 then
			exception.raise('fatal', 'peer id seems exhausted')
		end
		if peer_id_seed > _M.MAX_LOCAL_PEER_ID then
			start = start - peer.MAX_LOCAL_PEER_ID -- keep above restriction valid
			peer_id_seed = 1
		end
	end
	assert(peer_id_seed > 0)
	self.local_peer_id = peer_id_seed
	-- because this connection closed before any newer connection accepted, 
	-- same peer_id_seed may assigned to different connection.
	peer_id_seed = peer_id_seed + 1 
end
function conn_index:peer_id()
	return _M.make_id(self.local_peer_id, pulpo.thread_id, uuid.node_address)
end
function conn_index:local_peer_key()
	return tonumber(self.local_peer_id)
end
function conn_index:is_server()
	return self.local_peer_id ~= 0 
end
function conn_index:destroy(reason)
	peer_cmap[self:local_peer_key()] = nil	
	self.local_peer_id = 0
	conn_common_destroy(self, reason, cmap, conn_free_list) -- don't touch self after this function.
end
function conn_index:machine_id()
	return self.numeric_ipv4
end
function conn_index:address_family()
	return self.io:address().p[0].sa_family
end
function conn_index:cmapkey()
	return tonumber(self:machine_id())
end
function conn_index:alive()
	return self.dead ~= 1 
end
function conn_index:read_int(io, sr)
	local rb = self.rb
	local sup = sr:stream_unpacker(rb)
	while self:alive() and rb:read(io, 1024) do
		-- logger.notice('---------------------------- recv packet')
		-- rb:dump()
		while true do 
			local parsed, err_or_len = sr:unpack_packet(sup)
			--- logger.info('read_int', parsed, rb.hpos, rb.used)
			if not parsed then 
				if err_or_len then exception.raise('invalid', 'encoding', err_or_len) end
				break
			end
			router.internal(self, parsed, err_or_len)
		end
		rb:shrink_by_hpos()
	end
	sr:end_stream_unpacker(sup)
end
function conn_index:read_ext(io, unstrusted, sr)
	local rb = self.rb
	local sup = sr:stream_unpacker(rb)
	while self:alive() and rb:read(io, 1024) do
		while true do 
			--- logger.report('read_ext', rb.used, rb.hpos)
			local parsed, err_or_len = sr:unpack_packet(sup)
			if not parsed then 
				if err_or_len then exception.raise('invalid', 'encoding', err_or_len) end
				break
			end
			router.external(self, parsed, err_or_len, untrusted)
		end
		rb:shrink_by_hpos()
	end
	sr:end_stream_unpacker(sup)
end
local web_rb_work = memory.alloc_typed('luact_rbuf_t')
local fd_msgid_map = {}
function conn_index:read_webext(io, unstrusted, sr)
	local rb = web_rb_work
	while self:alive() do
		local buf = io:read()
		if not buf then break end -- close connection
		if self:is_server() then
			local verb, path, headers, body, blen = buf:raw_payload()
			-- print(path, headers, ffi.string(body, blen), headers:is_luact_agent())
			local p, method = path:match('(.*)/([^/]+)/?$')
			if not headers:is_luact_agent() then
				-- rest api call from normal web service. parsed is actual payload of rest api call
				local wrapped = {
					-- add special flag to indicate this is normal call
					headers:luact_msg_kind() or router.CALL,
					p, 
					headers:luact_msgid(), 
					method,
					[6] = verb,
					[7] = headers:as_table(),
					[8] = ffi.string(body, blen),
				}
				buf:fin()
				router.external(self, wrapped, 8, untrusted, true)
			else
				rb:from_buffer(body, blen)
				local ok, parsed, len = pcall(sr.unpack, sr, rb)
				buf:fin()
				if ok then
					if bit.band(parsed[1], router.NOTICE_MASK) ~= 0 then
						table.insert(parsed, 2, p)
						table.insert(parsed, 2, method)
					else
						local null_context = (parsed[3] == nil)
						if null_context then
							parsed[3] = false -- if nil, insert not works correctly
						end
						table.insert(parsed, 2, p)
						table.insert(parsed, 4, method)
						if null_context then
							parsed[5] = nil -- for full compatible argument with normal actor RPC
						end
					end
					router.external(self, parsed, len + 2, untrusted)
				else
					exception.raise('invalid', 'encoding', parsed)
				end
			end
		else -- client. receive response
			local status, headers, body, blen = buf:raw_payload()
			local msgid = fd_msgid_map[io:nfd()]
			fd_msgid_map[io:nfd()] = nil
			if msgid then
				-- reply from normal web service. msgid cannot use.
				-- HTTP 1.x : only 1 request at a time, so create fd - msgid map to retrieve msgid
				-- HTTP 2 : stream_id seems to be able to use as msgid. => TODO
				local ok = status == 200
				if ok then
					r = buf
				else
					r = exception.new('http', 'status', status)
					buf:fin()
				end
				local wrapped = {
					router.RESPONSE,
					msgid, ok, r
				}
				router.external(self, wrapped, 4, untrusted)
			else
				rb:from_buffer(body, blen)
				local ok, parsed, len = pcall(sr.unpack, sr, rb)
				buf:fin()
				if ok then
					router.external(self, parsed, len, untrusted)
				else
					logger.warn("invalid payload received:", ('%q'):format(ffi.string(body, blen)))
				end
			end				
		end
	end
end
function conn_index:write(io)
	local wb = self.wb
	wb:set_io(io)
	while self:alive() do
		wb:write()
	end
end
local conn_writer = assert(pbuf.writer.serde)
local prefixes = actor.prefixes
local function common_dispatch(self, sent, id, t, ...)
	local r
	t.id = nil -- release ownership of this table
	if not self:alive() then exception.raise('invalid', 'dead connection', tostring(self)) end
	local args_idx = 1
	local ctx = tentacle.get_context()
	if sent then args_idx = args_idx + 1 end
	if bit.band(t.flag, prefixes.timed_) ~= 0 then
		ctx = ctx or {}
		ctx[router.CONTEXT_TIMEOUT] = clock.get() + select(args_idx, ...)
		args_idx = args_idx + 1
	end
	if bit.band(t.flag, prefixes.__actor_) ~= 0 then
		if bit.band(t.flag, prefixes.notify_) ~= 0 then
			return self:notify_sys(id, t.method, select(args_idx, ...))
		elseif bit.band(t.flag, prefixes.async_) ~= 0 then
			return tentacle(self.async_sys, self, id, t.method, ctx, select(args_idx, ...))
		else
			return self:strip_result(self:sys(id, t.method, ctx, select(args_idx, ...)))
		end
	end
	if sent then
		if bit.band(t.flag, prefixes.notify_) ~= 0 then
			return self:notify_send(id, t.method, select(args_idx, ...))
		elseif bit.band(t.flag, prefixes.async_) ~= 0 then
			return tentacle(self.async_send, self, id, t.method, ctx, select(args_idx, ...))
		else
			return self:strip_result(self:send(id, t.method, ctx, select(args_idx, ...)))
		end
	else
		if bit.band(t.flag, prefixes.notify_) ~= 0 then
			return self:notify_call(id, t.method, select(args_idx, ...))
		elseif bit.band(t.flag, prefixes.async_) ~= 0 then
			return tentacle(self.async_call, self, id, t.method, ctx, select(args_idx, ...))
		else
			return self:strip_result(self:call(id, t.method, ctx, select(args_idx, ...)))
		end
	end
end
function conn_index:strip_result(ok, ...)
	if not ok then 
		error(({...})[1]) 
	end
	return ...
end
function conn_index:dispatch(t, ...)
	-- print('conn:dispatch', t.id, ({...})[1], t.id == select(1, ...))
	return common_dispatch(self, t.id == select(1, ...), uuid.local_id(t.id), t, ...)	
end
-- normal family
function conn_index:send(serial, method, ctx, ...)
	return self:send_and_wait(router.SEND, serial, method, ctx, ...)
end
function conn_index:call(serial, method, ctx, ...)
	return self:send_and_wait(router.CALL, serial, method, ctx, ...)
end
function conn_index:sys(serial, method, ctx, ...)
	return self:send_and_wait(router.SYS, serial, method, ctx, ...)
end
function conn_index:send_and_wait(cmd, serial, method, ctx, ...)
-- logger.info('send_and_wait', cmd, serial, method, ctx, debug.traceback())
	local msgid = router.regist(tentacle.running(), ctx and ctx[router.CONTEXT_TIMEOUT] or (clock.get() + _M.DEFAULT_TIMEOUT))
	self:rawsend(cmd, serial, msgid, method, ctx, ...)
	return tentacle.yield(msgid)
end


-- async family
function conn_index:async_send(serial, method, ctx, ...)
	return self:strip_result(self:send(serial, method, ctx, ...))
end
function conn_index:async_call(serial, method, ctx, ...)
	return self:strip_result(self:call(serial, method, ctx, ...))
end
function conn_index:async_sys(serial, method, ctx, ...)
	return self:strip_result(self:sys(serial, method, ctx, ...))
end

-- notify faimily 
function conn_index:notify_call(serial, method, ...)
	self:rawsend(router.NOTICE_CALL, serial, method, ...)
end
function conn_index:notify_send(serial, method, ...)
	self:rawsend(router.NOTICE_SEND, serial, method, ...)
end
function conn_index:notify_sys(serial, method, ...)
	self:rawsend(router.NOTICE_SYS, serial, method, ...)
end

-- response family
function conn_index:resp(msgid, local_peer_key, ...)
	if local_peer_key ~= self:local_peer_key() then
		logger.info('peer_id not match: connection closed before response arrived', local_peer_key, self:local_peer_key())
		return
	end
	self:rawsend(router.RESPONSE, msgid, ...)
end
function conn_index:http_resp(msgid, local_peer_key, ...)
	if local_peer_key ~= self:local_peer_key() then
		logger.info('peer_id not match: connection closed before response arrived', local_peer_key, self:local_peer_key())
		return
	end
	self:rawsend(bit.bor(router.REST_CALL_MASK, router.RESPONSE), msgid, ...)
end

-- direct send
function conn_index:rawsend(...)
-- logger.warn('conn:rawsend', self.io:address(), ...)
	if self.http == 0 then
		self.wb:send(conn_writer, self:serde(), ...)
	else
		local _kind = ...
		local kind = bit.band(_kind, router.CALL_MASK)
		self.wb.curr:reset()
		if kind ~= router.RESPONSE then
			if bit.band(kind, router.NOTICE_MASK) ~= 0 then
				exception.raise('invalid', "TODO: support notification via http by using http2 server push")
			else
				local _, path, msgid, method = ...
				local verb = method:match('^([A-Z]+)$')
				if verb then
					-- for normal web service. acts like normal request with 1 table (payload)
					local _, _, _, _, _, p, h, t = ...
					if not p then
						exception.raise('invalid', 'for REST request as RPC requires path')
					end
					if not t then t = h; h = {} end
					h[1] = verb; h[2] = path..p
					local _, _, address = _M.parse_hostname(ffi.string(self.hostname))
					h.host = address
					h["User-Agent"] = "Luact-No-RPC:"..tostring(kind)
					if t then -- pack body if any
						self:serde():pack(self.wb.curr, t)
					end
					-- when response is received at read_webext, can lookup msgid from request.
					-- HTTP 1 => only 1 request per connection at a time. so create fd - msgid map.
					-- HTTP 2 => set stream_id as msgid (TODO)
					if fd_msgid_map[self.io:nfd()] then
						logger.report('connection already send http request?', self.io:nfd(), fd_msgid_map[self.io:nfd()])
					end
					fd_msgid_map[self.io:nfd()] = msgid
					self.io:write(self.wb.curr:start_p(), self.wb.curr:available(), h)
				else 
					-- select(5, ...) unpacks context, arg1, arg2, ...
					-- select('#', ...) - 2 means whole argument minus UUID and METHOD (which packed as request path)
					self:serde().pack_vararg(self.wb.curr, {kind, msgid, select(5, ...)}, select('#', ...) - 2)
					self.io:write(self.wb.curr:start_p(), self.wb.curr:available(), {"POST", path.."/"..method})
				end
			end
		elseif bit.band(_kind, router.REST_CALL_MASK) ~= 0 then
			-- kind, msgid, result (true/false), arg1, ..., argN
			local _, _, ok = ...
			if ok then
				self:serde().pack_vararg(self.wb.curr, {select(4, ...)}, select('#', ...) - 3)
				self.io:write(self.wb.curr:start_p(), self.wb.curr:available())
			else
				self:serde().pack_vararg(self.wb.curr, {select(4, ...)}, select('#', ...) - 3)
				self.io:write(self.wb.curr:start_p(), self.wb.curr:available(), { 500 })
			end
		else
			self:serde().pack_vararg(self.wb.curr, {kind, select(2, ...)}, select('#', ...))
			self.io:write(self.wb.curr:start_p(), self.wb.curr:available())
		end
	end
end
ffi.metatype('luact_conn_t', conn_mt)

-- create remote connections
local function allocate_conn()
	local c = table.remove(conn_free_list)
	if not c then
		c = memory.alloc_typed('luact_conn_t')
		c:init_buffer()
	end
	return c
end
local function new_internal_conn(machine_id, opts)
	local c = allocate_conn()
	c:new(machine_id, opts)
	cmap[c:cmapkey()] = c
	return c
end


--[[
 	external connection instance and manager
--]]
local ext_conn_index = pulpo.util.copy_table(conn_index)
local ext_conn_free_list = {}
local ext_conn_mt = {
	__index = ext_conn_index,
}
function ext_conn_index:new(hostname, opts)
	self.hostname = memory.strdup(hostname)
	self.io,self.serde_id,self.http = open_io(hostname, opts)
	self.dead = 0
	self.local_peer_id = 0
	self:start_io(opts, self:serde())
	return self
end
function ext_conn_index:new_server(io, opts)
	self.hostname = memory.strdup(socket.inet_namebyhost(self.io:address().p))
	self.io = io
	self.serde_id = serde.kind[opts.serde or _M.DEFAULT_SERDE]
	self.http = opts.http and 1 or 0
	self.dead = 0
	self:assign_local_peer_id()
	self:start_io(opts, self:serde(), true)
	return self
end
-- for vid, 
function ext_conn_index:dispatch(t, ...)
	return common_dispatch(self, t.id == select(1, ...), t.id.path, t, ...)
end
function ext_conn_index:cmapkey()
	return ffi.string(self.hostname)
end
function ext_conn_index:destroy(reason)
	peer_cmap[self:local_peer_key()] = nil	
	self.local_peer_id = 0
	conn_common_destroy(self, reason, cmap, ext_conn_free_list) -- don't touch self after this function.
	print('h = ', h)
	memory.free(h)
	print('h freed')
end

ffi.metatype('luact_ext_conn_t', ext_conn_mt)

-- create external connections
local function allocate_ext_conn()
	local c = table.remove(ext_conn_free_list)
	if not c then
		c = memory.alloc_typed('luact_ext_conn_t')
		c:init_buffer()
	end
	return c
end
local function new_external_conn(hostname, opts)
	local c = allocate_ext_conn()
	c:new(hostname, opts)
	cmap[hostname] = c
	return c
end
local function new_server_conn(io, opts)
	local c
	local af = io:address().p[0].sa_family
	if af == AF_INET then
		c = allocate_conn()
		c:new_server(io, opts)
	elseif af == AF_INET6 then
		c = allocate_ext_conn()
		c:new_server(io, opts)
	else
		exception.raise('invalid', 'unsupported address family', af)
	end
	-- it is possible that more than 2 connection which has same ip address from external.
	-- OTOH there is only 1 connection established to communicate other machine in server cluster, 
	-- currently only internal connection will be cached to reduce total number of connection
	if opts.internal then
		cmap[c:cmapkey()] = c
	elseif not opts.volatile_connection then
		peer_cmap[c:local_peer_key()] = c
	end
	return c
end


--[[
 	local connection instance and manager
--]]

-- local conn metatable
local local_conn_index = pulpo.util.copy_table(conn_index)
local local_conn_free_list = {}
local local_conn_mt = {
	__index = local_conn_index,
}
function local_conn_index:start_io(opts, sr, reader, writer)
	local web, rev
	wev = tentacle(self.write, self, writer)
	rev = tentacle(self.read_int, self, reader, sr)
	tentacle(self.sweeper, self, wev, rev)
	conn_tasks:add(self) -- start keeping alive
end
local function make_channel_name(id1, id2)
	-- like 1_1, 1_2, 1_3, .... x_1, x_2, ... x_y (for all x, y <= n_threads)
	return tostring(id1).."_"..tostring(id2)
end
function local_conn_index:new_local(thread_id, opts)
	self.thread_id = thread_id
	self.dead = 0
	self.serde_id = serde.kind[opts.serde or _M.DEFAULT_SERDE]
	-- TODO : this uses too much fd (1 inter thread connection = 4 fd). should use unix domain socket?
	self.mine, self.yours = 
		linda.new(make_channel_name(pulpo.thread_id, thread_id)),
		linda.new(make_channel_name(thread_id, pulpo.thread_id))
	self:start_io(opts, self:serde(), self.mine:reader(), self.yours:writer())
	return self
end
function local_conn_index:task()
	-- get stat of other threads
	-- local ra = actor.root_of(nil, self.thread_id)
	-- _M.stats[mid] = ra:stat() -- used as heartbeat message
	-- TODO : check which thread accept this connection and use corresponding root actor.
	-- change second argument of actor.root_of
end
function local_conn_index:close()
	conn_tasks:remove(self)
	self.dead = 1
	self.mine:close()
	self.yours:close()
end
function local_conn_index:cmapkey()
	return tonumber(self.thread_id)
end
function local_conn_index:destroy(reason)
	conn_common_destroy(self, reason, lcmap, local_conn_free_list) -- after this function don't touch self
end
function local_conn_index:peer_id()
	return 0
end
function local_conn_index:local_peer_key()
	return 0
end
function local_conn_index:rawsend(...)
-- logger.warn('conn:rawsend', self.thread_id, ...)
	self.wb:send(conn_writer, self:serde(), ...)
end
ffi.metatype('luact_local_conn_t', local_conn_mt)

-- create local connections
local function allocate_local_conn()
	local c = table.remove(local_conn_free_list)
	if not c then
		c = memory.alloc_typed('luact_local_conn_t')
		c:init_buffer()
	end
	return c
end
local function new_local_conn(thread_id, opts)
	local c = allocate_local_conn()
	c:new_local(thread_id, opts)
	lcmap[thread_id] = c
	return c
end


--[[
	peer object ()
--]]
local peer_mt = pulpo.util.copy_table(conn_index)
local peer_free_list = {}
peer_mt.__index = peer_mt
function peer_mt:send_and_wait(cmd, serial, method, ctx, ...)
	local sent_ctx = ctx and util.copy_table(ctx) or {}
	sent_ctx[router.CONTEXT_PEER_ID] = nil -- remove peer_id, it no more used.
	return actor.root_of(self.detail.machine_id, self.detail.thread_id).push(self.detail.local_peer_id, 
		cmd, serial, method, sent_ctx, ...)
end
function peer_mt:close()
	return actor.root_of(self.detail.machine_id, self.detail.thread_id).close_peer(self.detail.local_peer_id)
end
-- rawsend is used for notify_send/call/sys family. so does not contain ctx.
function peer_mt:rawsend(cmd, ...)
	assert(bit.band(cmd, router.NOTICE_MASK) ~= 0)
	actor.root_of(self.detail.machine_id, self.detail.thread_id).push(self.detail.local_peer_id, cmd, ...)	
end
function peer_mt:dispatch(t, ...)
	-- print('conn:dispatch', t.id, ({...})[1], t.id == select(1, ...))
	return common_dispatch(self, t.id == select(1, ...), t.id.path, t, ...)
end
function peer_mt:alive() 
	return true
end
function peer_mt:__gc()
	table.insert(peer_free_list, self)
end
ffi.metatype('luact_peer_t', peer_mt)
local function allocate_peer()
	if #peer_free_list > 0 then
		return table.remove(peer_free_list)
	else
		return memory.alloc_typed('luact_peer_t')
	end
end


--[[
 	module functions
--]]
-- get default hostname to access given actor uuid
local internal_hostname_buffer = {}
function _M.internal_hostname_of(id)
	return _M.internal_hostname_by_addr(uuid.addr(id))
end
function _M.internal_hostname_by_addr(numeric_ipv4)
	internal_hostname_buffer[2] = socket.host_by_numeric_ipv4_addr(numeric_ipv4)
	return table.concat(internal_hostname_buffer)
end

-- create peer object (represent client which initiates current coroutine's execution)
function _M.new_peer(peer_id)
	local p = allocate_peer()
	p.peer_id = peer_id
	return p
end
local make_id_work = memory.alloc_typed('luact_peer_t')
function _M.make_id(local_peer_id, thread_id, machine_id)
	make_id_work.detail.local_peer_id = local_peer_id
	make_id_work.detail.thread_id = thread_id
	make_id_work.detail.machine_id = machine_id
	return make_id_work.peer_id
end


-- initialize
function _M.initialize(opts)
	internal_hostname_buffer[1] = (opts.internal_proto.."://")
	internal_hostname_buffer[3] = (":"..tostring(opts.internal_port))
	_M.opts = opts
	_M.use_connection_cache = true -- opts.use_connection_cache
	-- open connection for my thread
	for i=1,tonumber(opts.n_core or util.n_cpu()) do
		new_local_conn(i, opts)
	end
end

-- socket options for created connection
_M.opts = false

-- get (or create) connection to the node which id is exists.
function _M.get(id)
	if uuid.owner_machine_of(id) then
		return _M.get_by_thread_id(uuid.thread_id(id))
	else
		return _M.get_by_machine_id(uuid.addr(id))
	end
end

-- connect to node in same cluster by its internal ipv4 address
function _M.get_by_machine_id(machine_id)
	local c = cmap[machine_id]
	if not c then
		c = new_internal_conn(machine_id, _M.opts)
	end
	return c
end

-- connect to node in internet by specified hostname (include scheme like http+json-rpc:// or tcp://)
function _M.get_by_hostname(hostname)
	local c = cmap[hostname]
	if not c then
		c = new_external_conn(hostname, _M.opts)
	end
	return c
end
function _M.find_by_hostname(hostname)
	return cmap[hostname]
end
function _M.get_by_peer_id(peer_id)
	return peer_cmap[peer_id]
end

-- connect to another thread in same node
function _M.get_by_thread_id(thread_id)
	local c = lcmap[thread_id]
	if not c then
		c = new_local_conn(thread_id, _M.opts)
	end
	return c
end

-- create and cache connection from accepted io 
function _M.from_io(io, opts)
	return new_server_conn(io, opts)
end

return _M
