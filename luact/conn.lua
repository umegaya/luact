local pbuf = require 'luact.pbuf'
local read, write = pbuf.read, pbuf.write
local serde = require 'luact.serde'
local uuid = require 'luact.uuid'
local sleeper = require 'luact.sleep'
local actor = require 'luact.actor'
local msgidgen = require 'luact.msgid'
local supervise = require 'luact.supervise'
local router = require 'luact.router'

local _M = {}
_M.DEFAULT_SERDE = "serpent"
local thread = require 'pulpo.thread'
local socket

thread.add_initializer(function (loader, shmem)

local tentacle = require 'pulpo.tentacle'
local exception = require 'pulpo.exception'
local memory = require 'pulpo.memory'
socket = require 'pulpo.socket'
local gen = require 'pulpo.generics'

ffi.cdef [[
	typedef struct luact_conn {
		union {
			pulpo_addrinfo_t buf;
			struct sockaddr p;
		} addr;
		luact_io_t *io;
		luact_wbuf_t *wb;
		unsinged char serde_id, dead, padd[2];
	} luact_conn_t;
	typedef struct luact_local_conn {
		int thread_id;
		luact_pipe_io_t *io;
		luact_wbuf_t *wb;
		unsinged char serde_id, dead, padd[2];
	} luact_local_conn_t;
]]

local AF_INET = ffi.defs.AF_INET
local AF_INET6 = ffi.defs.AF_INET6


--[[
 	connection cache by thread_id or url
--]]
-- luact_conn_map_t
ffi.cdef(([[
	typedef struct luact_conn_map {
		%s list;
	} luact_conn_map_t;
]]):format(gen.mutex_ptr(gen.erastic_hash_map('luact_conn_t*'))))
local conn_map_mt = {
	cache = {}
}
function conn_map_mt.__newindex(t, k, v)
	local r = t.list:touch(function (data, key, ...)
		if select('#', ...) > 0 then
			return data:put(key, function (entry, ctor, ...)
				entry.data = ctor(entry.name, ...)
			end, ...)
		else
			return data:remove(key)
		end
	end, tostring(k), unpack(v))
	if v then
		conn_map_mt.cache[k] = r
	end
end
function conn_map_mt.__index(t, k)
	return conn_map_mt.cache[k]
end
ffi.metatype('luact_conn_map_t', conn_map_mt)

local cmap = ffi.new('luact_conn_map_t')
local lcmap = {}

--[[
 	remote conn metatable
--]]
local conn_index  = {}
local conn_mt = {
	__index = conn_index,
}
local function open_io(url, opts)
	local proto, sr, address = url:find('([^%+]+)%+?([^%+]*)://(.+)')
	if not proto then raise('invalid', 'url', url) end
	if #serde <= 0 then serde = _M.DEFAULT_SERDE end
	local p = pulpo.evloop.io[proto]
	assert(p.connect, exception.new('not_found', 'method', 'connect', proto))
	return p.connect(address, opts), serde.kind[sr]
end
local function start_io(c, untrusted, internal, sr)
	if internal then
		tentacle(c.read_int, c, sr)
	else
		tentacle(c.read_ext, c, untrusted, sr)
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
	start_io(self, not opts.internal, serde[self.serde_id])
	return self
end
function conn_index:new_server(io, opts)
	self.io = io
	self:store_peername()
	start_io(self, not opts.internal, serde[serde.kind[opts.serde]])
	return self
end
function conn_index:destroy(reason)
	cmap[self:cmapkey()] = nil
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
function conn_index:send(serial, method, flag, ...)
	-- TODO : apply flag setting
	local msgid = router.regist(coroutine.running())
	self.wb:send(conn_writer, sr, router.SEND, serial, msgid, method, ...)
	return coroutine.yield()
end
function conn_index:call(serial, method, flag, ...)
	-- TODO : apply flag setting
	local msgid = router.regist(coroutine.running())
	self.wb:send(conn_writer, serde[self.serde_id], router.CALL, serial, msgid, method, ...)
	return coroutine.yield()
end
function conn_index:notice_call(serial, method, flag, ...)
	-- TODO : apply flag setting
	self.wb:send(conn_writer, serde[self.serde_id], router.NOTICE_CALL, serial, msgid, method, ...)
end
function conn_index:notice_send(serial, method, flag, ...)
	-- TODO : apply flag setting
	self.wb:send(conn_writer, serde[self.serde_id], router.NOTICE_SEND, serial, msgid, method, ...)
end
function conn_index:resp(msgid, ...)
	self.wb:send(conn_writer, serde[self.serde_id], router.RESPONSE, msgid, ...)
end
-- attach to ctype
ffi.metatype('luact_conn_t', conn_mt)


--[[
 	local conn metatable
--]]
local local_conn_index  = {}
local local_conn_mt = {
	__index = local_conn_index,
}
function local_conn_index:new_local(thread_id, opts)
	self.thread_id = thread_id
	self.io = pulpo.evloop.linda.new(tostring(thread_id), opts)
	start_io(self, false, serde.get(opts.serde))
	return self
end
function local_conn_index:destroy(reason)
	cmap[self.thread_id] = nil
	self.dead = 1
end
-- same as conn_index's
local_conn_index.read = conn_index.read
local_conn_index.write = conn_index.write
local_conn_index.send = conn_index.send

-- attach to ctype
ffi.metatype('luact_local_conn_t', local_conn_mt)

end) -- thread.add_initializer

--[[
 	factory method (local)
--]]
local function new_conn(addr, opts)
	cmap[addr] = {
		function (key, options)
			local c = memory.alloc_typed('luact_conn_t')
			c:new(key, options)
			return c
		end, opts
	}
	return cmap[addr]
end
local function new_local_conn(thread_id, opts)
	local c = memory.alloc_typed('luact_local_conn_t')
	c:new_local(thread_id, opts)
	lcmap[thread_id] = c
	return c
end
local function new_server_conn(io, opts)
	local c = memory.alloc_typed('luact_conn_t')
	c:new_server(io, opts)
	-- reuse server connection for querying client node (in the cluster)
	if opts.internal then
		cmap[c:cmapkey()] = {
			function (key, ptr) 
				return ptr 
			end, c
		}
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
	local c = cmap[machine_id]
	if not c then
		sv = supervise(new_internal_conn, machine_id, _M.opts)
		c = cmap[url]
	end
	return c	
end

function _M.get_by_url(url)
	local c = cmap[url]
	if not c then
		sv = supervise(new_conn, url, _M.opts)
		c = cmap[url]
	end
	return c
end
function _M.get_by_thread_id(thread_id)
	local c = lcmap[thread_id]
	if not c then
		supervise(new_local_conn, thread_id, _M.opts)
		c = lcmap[thread_id]
	end
	return c
end

-- create and cache connection from accepted io 
function _M.from_io(io, opts)
	local c = actor.new(new_server_conn, io, opts)
	return actor.body_of(c)
end

return _M
