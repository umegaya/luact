local ffi = require 'ffiex.init'

local pulpo = require 'pulpo.init'

local pbuf = require 'luact.pbuf'
local read, write = pbuf.read, pbuf.write
local serde = require 'luact.serde'
-- serde.DEBUG = true
local uuid = require 'luact.uuid'
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
		unsigned char serde_id, dead;
		unsigned short task_processor_id;
	} luact_conn_t;
	typedef struct luact_ext_conn {
		pulpo_io_t *io;
		luact_rbuf_t rb;
		luact_wbuf_t wb;
		unsigned char serde_id, dead;
		unsigned short task_processor_id;
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

local AF_INET = ffi.defs.AF_INET
local AF_INET6 = ffi.defs.AF_INET6


--[[
 	connection managers (just hash map)
--]]
local cmap, lcmap = {}, {}
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
			table.remove(i)
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
	-- TODO : if user and credential is specified, how should we handle these?
	local p = pulpo.evloop.io[proto]
	assert(p.connect, exception.new('not_found', 'method', 'connect', proto))
	return p.connect(address, opts), serde.kind[sr]
end
function conn_index:init_buffer()
	self.rb:init()
	self.wb:init()
end
function conn_index:start_io(opts, sr, server)
	local rev, wev
	if opts.internal then
		rev = tentacle(self.read_int, self, self.io, sr)
	else
		rev = tentacle(self.read_ext, self, self.io, true, sr)
	end
	wev = tentacle(self.write, self, self.io)
	tentacle(self.sweeper, self, rev, wev)
	-- for example, normal http (not 2.0) is volatile.
	if not (server or opts.volatile_connection) then
		conn_tasks:add(self) -- start keeping alive
	else
		self.task_processor_id = 0 -- no task
	end
end
function conn_index:sweep()
	machine_stats[self:machine_id()] = nil
	conn_tasks:remove(self)
end
function conn_index:sweeper(rev, wev)
	local tp,obj = event.wait(nil, rev, wev)
	-- these 2 line assures another tentacle (read for write/write for read)
	-- start to finish.
	self:close()
	-- CAUTION: we never wait all tentacle related with this connection finished.
	-- that depend on our conn_index:write, read_int, read_ext implementation, 
	-- which once io:close called, above coroutine are immediately finished.
	-- if this fact is changed, please wait for termination of all tentacles.
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
	self.io,self.serde_id = open_io(hostname, opts)
	self.dead = 0
	self:start_io(opts, self:serde())
	return self
end
function conn_index:new_server(io, opts)
	self.io = io
	self.serde_id = serde.kind[opts.serde or _M.DEFAULT_SERDE]
	self.dead = 0
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
	if map[self:cmapkey()] ~= self then
		assert(false, "connection not match:"..tostring(self).." and "..tostring(map[self:cmapkey()]))
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
	end
end
function conn_index:destroy(reason)
	conn_common_destroy(self, reason, cmap, conn_free_list)
end
function conn_index:machine_id()
	assert(self:address_family() == AF_INET)
	return self.io:addrinfo().addr4.sin_addr.s_addr
end
function conn_index:address_family()
	return self.io:addrinfo().addrp[0].sa_family
end
function conn_index:cmapkey()
	local af = self:address_family()
	if af == AF_INET then
		return self:machine_id()
	elseif af == AF_INET6 then
		return socket.inet_namebyhost(self.io:addrinfo().addrp)
	else
		exception.raise('invalid', 'address', 'family', af)
	end
end
function conn_index:read_int(io, sr)
	local rb = self.rb
	while self.dead ~= 1 do
		rb:read(io, 1024) 
		while true do 
			local parsed, err = sr:unpack_packet(rb)
			if not parsed then 
				if err then exception.raise('invalid', 'encoding', err) end
				break
			end
			router.internal(self, parsed)
		end
		rb:shrink_by_hpos()
	end
end
function conn_index:read_ext(io, unstrusted, sr)
	local rb = self.rb
	while self.dead ~= 1 do
		rb:read(io, 1024) 
		while true do 
			local parsed, err = sr:unpack_packet(rb)
			if not parsed then 
				if err then exception.raise('invalid', 'encoding', err) end
				break
			end
			router.external(self, parsed, untrusted)
		end
		rb:shrink_by_hpos()
	end
end
function conn_index:write(io)
	local wb = self.wb
	wb:set_io(io)
	while self.dead ~= 1 do
		wb:write()
	end
end
local conn_writer = assert(pbuf.writer.serde)
local prefixes = actor.prefixes
local function strip_result(...)
	local r = {...}
	if not r[1] then error(r[2]) end
	return unpack(r, 2)	
end
local function common_dispatch(self, sent, id, t, ...)
	local r
	t.id = nil -- release ownership of this table
	if self.dead ~= 0 then exception.raise('invalid', 'dead connection', tostring(self)) end
	local args_idx = 1
	if sent then args_idx = args_idx + 1 end
	local timeout = _M.DEFAULT_TIMEOUT
	if bit.band(t.flag, prefixes.timed_) ~= 0 then
		timeout = select(args_idx, ...)
		args_idx = args_idx + 1
	end
	if bit.band(t.flag, prefixes.__actor_) ~= 0 then
		if bit.band(t.flag, prefixes.notify_) ~= 0 then
			return self:notify_sys(id, t.method, select(args_idx, ...))
		elseif bit.band(t.flag, prefixes.async_) ~= 0 then
			return tentacle(self.async_sys, self, id, t.method, timeout, select(args_idx, ...))
		else
			return strip_result(self:sys(id, t.method, timeout, select(args_idx, ...)))
		end
	end
	if sent then
		if bit.band(t.flag, prefixes.notify_) ~= 0 then
			return self:notify_send(id, t.method, select(args_idx, ...))
		elseif bit.band(t.flag, prefixes.async_) ~= 0 then
			return tentacle(self.async_send, self, id, t.method, timeout, select(args_idx, ...))
		else
			return strip_result(self:send(id, t.method, timeout, select(args_idx, ...)))
		end
	else
		if bit.band(t.flag, prefixes.notify_) ~= 0 then
			return self:notify_call(id, t.method, select(args_idx, ...))
		elseif bit.band(t.flag, prefixes.async_) ~= 0 then
			return tentacle(self.async_call, self, id, t.method, timeout, select(args_idx, ...))
		else
			return strip_result(self:call(id, t.method, timeout, select(args_idx, ...)))
		end
	end
end
function conn_index:dispatch(t, ...)
	-- print('conn:dispatch', t.id, ({...})[1], t.id == select(1, ...))
	return common_dispatch(self, t.id == select(1, ...), uuid.local_id(t.id), t, ...)
end
function conn_index:vdispatch(t, ...)
	return common_dispatch(self, t.id == select(1, ...), t.id.path, t, ...)
end
-- normal family
function conn_index:send(serial, method, timeout, ...)
	local msgid = router.regist(tentacle.running(), timeout)
	self.wb:send(conn_writer, self:serde(), router.SEND, serial, msgid, method, ...)
	return tentacle.yield(msgid)
end
function conn_index:call(serial, method, timeout, ...)
	local msgid = router.regist(tentacle.running(), timeout)
	self.wb:send(conn_writer, self:serde(), router.CALL, serial, msgid, method, ...)
	return tentacle.yield(msgid)
end
function conn_index:sys(serial, method, timeout, ...)
	local msgid = router.regist(tentacle.running(), timeout)
	self.wb:send(conn_writer, self:serde(), router.SYS, serial, msgid, method, ...)
	return tentacle.yield(msgid)
end

-- async family
function conn_index:async_send(serial, method, timeout, ...)
	return strip_result(self:send(serial, method, timeout, ...))
end
function conn_index:async_call(serial, method, timeout, ...)
	return strip_result(self:call(serial, method, timeout, ...))
end
function conn_index:async_sys(serial, method, timeout, ...)
	return strip_result(self:sys(serial, method, timeout, ...))
end

-- notify faimily 
function conn_index:notify_call(serial, method, ...)
	self.wb:send(conn_writer, self:serde(), router.NOTICE_CALL, serial, method, ...)
end
function conn_index:notify_send(serial, method, ...)
	self.wb:send(conn_writer, self:serde(), router.NOTICE_SEND, serial, method, ...)
end
function conn_index:notify_sys(serial, method, ...)
	self.wb:send(conn_writer, self:serde(), router.NOTICE_SYS, serial, method, ...)
end

-- response family
function conn_index:resp(msgid, ...)
	self.wb:send(conn_writer, self:serde(), router.RESPONSE, msgid, ...)
end

-- direct send
function conn_index:rawsend(...)
	self.wb:send(conn_writer, self:serde(), ...)
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
local function new_server_conn(io, opts)
	local c = allocate_conn()
	c:new_server(io, opts)
	if opts.internal and (not cmap[c:cmapkey()]) then
		-- it is possible that more than 2 connection which has same ip address from external.
		-- OTOH there is only 1 connection required to communicate other machine in server cluster, 
		-- only internal connection will be cached.
		cmap[c:cmapkey()] = c
	end
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
	self.io,self.serde_id = open_io(hostname, opts)
	self.dead = 0
	self:start_io(opts, self:serde())
	return self
end
function ext_conn_index:cmapkey()
	return ffi.string(self.hostname)
end
function ext_conn_index:destroy(reason)
	conn_common_destroy(self, reason, cmap, ext_conn_free_list)
	memory.free(self.hostname)
end
ffi.metatype('luact_ext_conn_t', ext_conn_mt)

-- create external connections
local function allocate_ext_conn()
	local c = table.remove(local_conn_free_list)
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
	rev = tentacle(self.read_int, self, reader, sr)
	wev = tentacle(self.write, self, writer)
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
	-- TODO : this uses too much fd (1 connection = 4 fd). should use unix domain socket?
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
	conn_common_destroy(self, reason, lcmap, local_conn_free_list)
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

-- initialize
function _M.initialize(opts)
	internal_hostname_buffer[1] = (opts.internal_proto.."://")
	internal_hostname_buffer[3] = (":"..tostring(opts.internal_port))
	_M.opts = opts
	_M.use_connection_cache = opts.use_connection_cache
	-- open connection for my thread
	for i=1,tonumber(opts.n_core or util.n_cpu()) do
		new_local_conn(i, opts)
	end
end

-- socket options for created connection
_M.opts = false

-- get (or create) connection to the node which id is exists.
function _M.get(id)
	if uuid.owner_of(id) then
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
