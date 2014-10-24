local ffi = require 'ffiex.init'

local pulpo = require 'pulpo.init'

local pbuf = require 'luact.pbuf'
local read, write = pbuf.read, pbuf.write
local serde = require 'luact.serde'
local uuid = require 'luact.uuid'
local clock = require 'luact.clock'
local actor = require 'luact.actor'
local msgidgen = require 'luact.msgid'
local supervise = require 'luact.supervise'
local router = require 'luact.router'

local thread = require 'pulpo.thread'
local tentacle = require 'pulpo.tentacle'
local exception = require 'pulpo.exception'
local memory = require 'pulpo.memory'
local gen = require 'pulpo.generics'
local socket = require 'pulpo.socket'
local linda = pulpo.evloop.io.linda

local _M = (require 'pulpo.package').module('luact.defer.conn_c')
_M.DEFAULT_SERDE = "serpent"

ffi.cdef [[
	typedef struct luact_conn {
		union {
			pulpo_addrinfo_t buf;
			struct sockaddr p;
		} addr;
		pulpo_io_t *io;
		luact_wbuf_t *wb;
		unsigned char serde_id, dead, padd[2];
	} luact_conn_t;
	typedef struct luact_local_conn {
		int thread_id;
		pulpo_pipe_io_t *io;
		luact_wbuf_t *wb;
		unsigned char serde_id, dead, padd[2];
	} luact_local_conn_t;
]]

local AF_INET = ffi.defs.AF_INET
local AF_INET6 = ffi.defs.AF_INET6


--[[
 	remote connection instance and manager
--]]
-- connection manager (map)
ffi.cdef(([[
	typedef struct luact_conn_map {
		%s list;
	} luact_conn_map_t;
]]):format(gen.mutex_ptr(gen.erastic_hash_map('luact_conn_t'))))
local conn_map_index = {}
local conn_map_mt = {
	__index = conn_map_index, 
	cache = {}
}
function conn_map_index.init(t, size)
	t.list:init(function (data) data:init(size) end)
end
function conn_map_index.put(t, k, fn, ...)
	local r = t.list:touch(function (data, key, ctor, ...)
		return data:put(key, function (entry, initializer, ...)
			initializer(entry, ...)
		end, ctor, ...)
	-- TODO : create hash map of numerical key version to reduce cost of tostring
	end, tostring(k), fn)
	-- update cache
	rawset(conn_map_mt.cache, k, r)
end
function conn_map_index.remove(t, k)
	t.list:touch(function (data, key)
		data:remove(key)
	-- TODO : create hash map of numerical key version to reduce cost of tostring
	end, tostring(k))
	-- update cache
	rawset(conn_map_mt.cache, k, nil)
end
function conn_map_index.get(t, k)
	return rawget(conn_map_mt.cache, k)
end
ffi.metatype('luact_conn_map_t', conn_map_mt)

local cmap = ffi.new('luact_conn_map_t')
cmap:init(pulpo.poller.config.maxfd)

-- remote conn metatable
local conn_index  = {}
local conn_mt = {
	__index = conn_index,
}
local function open_io(url, opts)
	local proto, sr, address = _M.urlparse(url)
	local p = pulpo.evloop.io[proto]
	assert(p.connect, exception.new('not_found', 'method', 'connect', proto))
	return p.connect(address, opts), serde.kind[sr]
end
local function start_io(c, internal, sr)
	if internal then
		tentacle(c.read_int, c, sr)
	else
		tentacle(c.read_ext, c, true, sr)
	end
	tentacle(c.write, c)
end
function conn_index:new_internal(addr, opts)
	-- numeric ipv4 address given
	url = _M.hostname_of(addr)
	return self:new(url, opts)
end
function conn_index:new(url, opts)
	self.io,self.serde_id = open_io(url, opts)
	self:store_peername()
	start_io(self, opts.internal, serde[self.serde_id])
	return self
end
function conn_index:new_server(io, opts)
	self.io = io
	self:store_peername()
	start_io(self, opts.internal, serde[serde.kind[opts.serde]])
	return self
end
function conn_index:destroy(reason)
	cmap:remove(self:cmapkey())
	self.dead = 1
	memory.free(self)
end
function conn_index:cmapkey()
	if self.addr.p.sa_family == AF_INET then
		return ffi.cast('struct sockaddr_in*', self.addr.p).sin_addr.in_addr
	elseif self.addr.p.sa_family == AF_INET6 then
		return socket.inet_namebyhost(self.addr.p)
	else
		exception.raise('invalid', 'address', 'family', self.addr.p.sa_family)
	end
end
function conn_index:store_peername()
	socket.inet_peerbyfd(self.io:fd(), self.addr.p, ffi.sizeof(self.addr.buf))
end
function conn_index:read_int(sr)
	local rb = read.new()
	while self.dead ~= 1 do
		rb:read(io, 1024) 
		while true do 
			parsed, err = sr:unpack(rb)
			if not parsed then 
				if err then
					io:close()
					exception.raise('invalid', 'encoding', err)
				end
				break
			end
			router.internal(self, parsed)
		end
	end
end
function conn_index:read_ext(untrusted, sr)
	local rb = read.new()
	while self.dead ~= 1 do
		rb:read(io, 1024) 
		while true do 
			parsed, err = sr:unpack(rb)
			if not parsed then 
				if err then
					io:close()
					exception.raise('invalid', 'encoding', err)
				end
				break
			end
			router.external(self, parsed, untrusted)
		end
	end
end
function conn_index:write()
	local wb = write.new()
	local io = self.io
	self.wb = wb
	while self.dead ~= 1 do
		wb:write(io)
	end
end
local conn_writer = pbuf.writer.serde
local prefixes = actor.prefixes
local function common_dispatch(sent, id, t, ...)
	local args_idx = 1
	if sent then args_idx = args_idx + 1 end
	if bit.band(t.flag, prefixes.__sys__) then
		if bit.band(t.flag, prefixes.notify_) then
			return self:notify_sys(id, t.method, select(args_idx, ...))
		elseif bit.band(t.flag, prefixes.async_) then
			return self:async_sys(id, t.method, select(args_idx, ...))
		else
			return self:sys(id, t.method, select(args_idx, ...))
		end
	end
	if sent then
		if bit.band(t.flag, prefixes.notify_) then
			return self:notify_send(id, t.method, select(args_idx, ...))
		elseif bit.band(t.flag, prefixes.async_) then
			return self:async_send(id, t.method, select(args_idx, ...))
		else
			return self:send(id, t.method, select(args_idx, ...))
		end
	else
		if bit.band(t.flag, prefixes.notify_) then
			return self:notify_call(id, t.method, select(args_idx, ...))
		elseif bit.band(t.flag, prefixes.async_) then
			return self:async_call(id, t.method, select(args_idx, ...))
		else
			return self:call(id, t.method, select(args_idx, ...))	
		end
	end
end
function conn_index:dispatch(t, ...)
	local r = {common_dispatch(t.uuid == select(1, ...), t.uuid:__serial(), t, ...)}
	if not r[1] then error(r[2]) end
	return unpack(r, 2)
end
function conn_index:vdispatch(t, ...)
	local r = {common_dispatch(t.vid == select(1, ...), t.vid.path, t, ...)}
	if not r[1] then error(r[2]) end
	return unpack(r, 2)
end
function conn_index:send(serial, method, timeout, ...)
	-- TODO : apply flag setting
	local msgid = router.regist(coroutine.running(), timeout)
	self.wb:send(conn_writer, sr, router.SEND, serial, msgid, method, ...)
	return coroutine.yield()
end
function conn_index:call(serial, method, timeout, ...)
	-- TODO : apply flag setting
	local msgid = router.regist(coroutine.running(), timeout)
	self.wb:send(conn_writer, serde[self.serde_id], router.CALL, serial, msgid, method, ...)
	return coroutine.yield()
end
function conn_index:sys(serial, method, timeout, ...)
	-- TODO : apply flag setting
	local msgid = router.regist(coroutine.running(), timeout)
	self.wb:send(conn_writer, serde[self.serde_id], router.SYS, serial, msgid, method, ...)
	return coroutine.yield()
end
function conn_index:notify_call(serial, method, ...)
	-- TODO : apply flag setting
	self.wb:send(conn_writer, serde[self.serde_id], router.NOTICE_CALL, serial, msgid, method, ...)
end
function conn_index:notify_send(serial, method, ...)
	-- TODO : apply flag setting
	self.wb:send(conn_writer, serde[self.serde_id], router.NOTICE_SEND, serial, msgid, method, ...)
end
function conn_index:notify_sys(serial, method, ...)
	-- TODO : apply flag setting
	self.wb:send(conn_writer, serde[self.serde_id], router.NOTICE_SYS, serial, msgid, method, ...)
end
function conn_index:resp(msgid, ...)
	self.wb:send(conn_writer, serde[self.serde_id], router.RESPONSE, msgid, ...)
end
-- attach to ctype
ffi.metatype('luact_conn_t', conn_mt)



--[[
 	local connection instance and manager
--]]

-- local conn metatable
local local_conn_index  = {}
local local_conn_mt = {
	__index = local_conn_index,
}
function local_conn_index:new_local(thread_id, opts)
	self.thread_id = thread_id
	self.io = linda.new(tostring(thread_id), opts)
	start_io(self, true, serde.get(opts.serde))
	return self
end
function local_conn_index:destroy(reason)
	lcmap[self.thread_id] = nil
	self.dead = 1
end
-- same as conn_index's
local_conn_index.read = conn_index.read
local_conn_index.write = conn_index.write
local_conn_index.send = conn_index.send

-- attach to ctype
ffi.metatype('luact_local_conn_t', local_conn_mt)

-- local connection manager
local function new_local_conn(thread_id, opts, cache)
	local c = memory.alloc_typed('luact_local_conn_t')
	c:new_local(thread_id, opts)
	rawset(cache, thread_id, c)
	return c
end
local lcmap = setmetatable({}, {
	__index = function (t, id)
		supervise(new_local_conn, id, _M.opts, t)
		return rawget(t, id)
	end,
})
_M.local_cmap = lcmap



--[[
 	factory method (remote)
--]]
local function new_conn(addr, opts)
	return cmap:put(addr, function (entry, options)
		entry.data:new(key, options)
	end, opts)
end
local function new_server_conn(io, opts)
	local c = memory.alloc_typed('luact_conn_t')
	c:new_server(io, opts)
	if opts.internal then
		local tmp = cmap:put(c:cmapkey(), function (entry, server_conn, opts)
			ffi.copy(entry.data, server_conn)
		end, io, opts)
		memory.free(c)
		c = tmp
	end
	return c
end

--[[
 	module functions
--]]
-- get default hostname to access given actor uuid
local hostname_buffer = {}
function _M.hostname_of(id)
	hostname_buffer[2] = socket.inet_namebyhost(id:__addr())
	return table.concat(hostname_buffer)
end

-- initialize
function opts_from_cmdl(cmdl_args)
	-- NYI
	return cmdl_args
end
function _M.initialize(cmdl_args)
	hostname_buffer[1] = cmdl_args.proto
	hostname_buffer[3] = (":"..tostring(cmdl_args.port))
	_M.opts = opts_from_cmdl(cmdl_args)
end

-- socket options for created connection
-- TODO : find a way to 
_M.opts = false

-- get (or create) connection to the node which id is exists.
function _M.get(id)
	if uuid.owner_of(id) then
		return _M.get_by_thread_id(id:__thread_id())
	else
		return _M.get_by_machine_id(id:__addr())
	end
end

function _M.get_by_machine_id(machine_id)
	local c = cmap:get(machine_id)
	if not c then
		sv = supervise(new_internal_conn, machine_id, _M.opts)
		c = cmap:get(url)
	end
	return c
end

function _M.get_by_url(url)
	local c = cmap:get(url)
	if not c then
		sv = supervise(new_conn, url, _M.opts)
		c = cmap:get(url)
	end
	return c
end
function _M.get_by_thread_id(thread_id)
	return lcmap[thread_id]
end

-- create and cache connection from accepted io 
function _M.from_io(io, opts)
	local c = actor.new(new_server_conn, io, opts)
	return actor.body_of(c)
end

return _M
