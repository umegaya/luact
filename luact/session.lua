local buf = require 'luact.buf'
local exception = require 'lib.pulpo.exception'
local memory = require 'lib.pulpo.memory'
local gen = require 'lib.pulpo.generics'
local raise = exception.raise
local event = pulpo.event

local C = ffi.C
local _M = {}

-- luact_session_t
ffi.cdef [[
	typedef struct luact_session {
		char *address, *proto;
		uint32_t owner_thread:12, is_local:1, padd:3, n_connect:16;
		union {
			luact_io_t *fd;
			luact_pipe_io_t *pipe;
		} io;
		luact_buf_t rb;
		luact_vec_t wb[2], *front, *back;
		pthread_mutex_t mutex[1];
	} luact_session_t;
]]
local session_index = {}
local coromap = {}
function session_index.init(t, url, is_local)
	local proto, address = url:find('(.+)://(.+)')
	if not proto then raise('invalid', 'url', url) end
	t.address = memory.strdup(address)
	t.proto = memory.strdup(proto)
	t.owner_thread = pulpo.thread_id
	t.is_local = is_local and 1 or 0
	t.rb:init()
	t.wb[0]:init(); t.front = t.wb
	t.wb[1]:init(); t.back = t.wb + 1
	C.pthread_mutex_init(t.mutex, nil)
end
function session_index.is_local_session(t)
	return t.is_local ~= 0
end
function session_index.call(t, id, method, ...)
	local msgid = new_id()
	coromap[msgid] = coroutine.running()
	t:write(id, msgid, method, {...})
	return coroutine.yield()
end
function session_index.send(t, id, method, ...)
	local msgid = new_id()
	coromap[msgid] = coroutine.running()
	t:write(id, msgid, method, {...})
	return coroutine.yield()
end
function session_index.write(t, ...)
	t.front:add(true, ...)
	if pulpo.thread_id == t.owner_thread then
		connect(t)
		t.wev:emit()
	else
		-- TODO : send emit between thread
	end
end
function session_index.raw_write(t, ptr, len)
	t.front:add(false, ptr, len)
	if pulpo.thread_id == t.owner_thread then
		connect(t)
		t.wev:emit()
	else
		-- TODO : send emit between thread
	end
end
function session_index.close(t)
	local io = t:is_local_session() and t.io.pipe or t.io.fd
	if io then 
		io:close()
	end
	t.addr = nil
	t.n_connect = 0
end
ffi.metatype('luact_session_t', { __index = session_index })



-- luact_session_manager_t
ffi.cdef(([[
	typedef struct luact_session_manager {
		%s list;
	} luact_session_manager_t;
]]):format(gen.mutex_ptr(gen.elastic_map('luact_session_t'))))
local session_manager_index = {
	cache = {}
}
function session_manager_index.find_or_new(t, url, is_local)
	local from_cache = session_manager_index.cache[url]
	if not from_cache then
		from_cache = t.list:touch(function (data, key, is_local_session)
			return data:put(key, function (entry)
				local s = memory.alloc_typed('luact_session_t')
				s:init(entry.name, is_local_session)
				entry.data = s
			end)
		end, name, is_local)
		session_manager_index.cache[url] = from_cache
		if url:find('linda://') == 0 then
			local_sessions[tonumber(from_cache.address)] = from_cache
		end
	end
	return from_cache
end
ffi.metatype('luact_session_manager_t', { __index = session_manager_index })
local session_manager = ffi.new('luact_session_manager_t')



-- local function
local MAX_CONN_RETRY = 1000
local connect

local function process_record_common(s, parsed)
	local a,msgid,method,co
	if parsed:is_request() then
		-- request: parsed = callee(short_id),msgid,method,{arg1,...,argN}
		a,msgid = actor.find(parsed:callee():serial_id()),parsed:msgid()
		if a then
			method = rawget(a, parsed:method())
			if method then
				pulpo.tentacle(function (_sock, _msgid, _method, _args)
					local _co = coroutine.running()
					actor.set_peer(_co, _msgid)
					_sock:write(_msgid,{pcall(_method, unpack(_args))})
					actor.set_peer(_co, nil)
				end, s, msgid, method, parsed:args())
			else
				s:write(msgid,{false, exception.new('not_found','method',parsed:method())})							
			end
		else
			s:write(msgid,{false, exception.new('not_found','actor',parsed:callee())})
		end
	elseif parsed:is_notify() then
		-- notify: parsed = callee(short_id),method,{arg1,...,argN}
		a = actor.find(parsed:callee():serial_id())
		if a then
			method = rawget(a, parsed:method())
			if method then
				pulpo.tentacle(function (_method, _args)
					pcall(_method, _args)
				end, method, parsed:args())
			end
		end
	elseif parsed:is_response() then
		-- response: parsed = msgid,{is_success,arg1,...,argN}
		co = table.remove(coromap, parsed:msgid())
		if co then
			coroutine.resume(co, parsed:args())
		end
	end
end

local function local_read_thread(s)
	local parsed,err,len
	local a,msgid,method
	local co
	local rb,map,io = s.rb,s.map,s.io.pipe
	while true do
		rb:reserve(1024)
		rb:read(io)
		while true do 
			parsed,err = rb:unpack()
			if not parsed then 
				if err then
					-- TODO : should die immediately
					return
				end
				break 
			end
			process_record_common(s, parsed)
		end
	end
end
local function remote_read_thread(s)
	local parsed,err,len
	local rb,map,io = s.rb,s.map,s.io.fd
	while true do
	::read_start::
		rb:reserve(1024)
		if not rb:read(io) then
		::reconnection::
			-- reconnection
			io = connect(s)
			if io then
				s.io.fd = io
				goto read_start
			end
			break
		end
		while true do 
			parsed, err = rb:unpack()
			if not parsed then 
				if err then
					io:close()
					goto reconnection
				end
				break 
			end
			if parsed:is_request() then
				if not parsed:callee():is_assigned_to_current_thread() then
					pulpo.tentacle(function (_sock, _actor, _msgid, _method, _args)
						_sock:write(_msgid,{pcall(_actor._method, unpack(_args))})
					end, s, parsed:callee(), parsed:msgid(), parsed:method(), parsed:args())
				else
					process_record_common(s, parsed)
				end
			elseif parsed:is_notify() then
				if not parsed:callee():is_assigned_to_current_thread() then
					local rep = local_sessions[parsed:thread_id()]
					rep:raw_write(parsed:vec())
				else
					process_record_common(s, parsed)
				end
			elseif not parsed:is_assigned_to_current_thread() then
				local rep = local_sessions[parsed:thread_id()]
				rep:raw_write(parsed:vec())
			else
				process_record_common(s, parsed)
			end
		end
	end
end
local function local_write_thread(so)
	local wb,wev,io = s.wb,s.wev,s.io.pipe
	local len
	while true do
	::write_start::
		if wb:size() <= 0 then
			event.wait(wev)
		end
		local len = wb:write(io)
		if not len then
			return
		else
			wb:shrink(len)
		end
	end
end
local function remote_write_thread(s)
	local wb,wev,io = s.wb,s.wev,s.io.fd
	local len
	while true do
	::write_start::
		if wb:size() <= 0 then
			event.wait(wev)
		end
		local len = wb:write(io)
		if not len then
			io = connect(s)
			if io then
				s.io.fd = io
				goto write_start
			end
			return
		else
			wb:shrink(len)
		end
	end
end
connect = function (t)
	-- stop all coroutine which wait for previous connection's response
	for _,co in pairs(t.map) do
		coroutine.resume(co, false)
	end
	-- if connection not attempt to closed, reconnect!
	if (not t.io) and t.addr then
		local io_factory = assert(pulpo.io[t.proto], exception.new('not_found', 'io', t.proto))
		local io = io_factory.new(t.addr)
		t.io.fd = ffi.cast('void *', io)
		if t.n_connect <= 0 then
			pulpo.tentacle(t:is_local_session() and local_read_thread or remote_read_thread, t)
			pulpo.tentacle(t:is_local_session() and local_write_thread or remote_write_thread, t)
		end
		if t.n_connect < MAX_CONN_RETRY then
			t.n_connect = t.n_connect + 1
		end
		return io
	end
	return nil
end

-- module function
function _M.get(url)
	return session_manager:find_or_new(url)
end

_M.my_addr = pulpo.util.getifaddr()

return _M
