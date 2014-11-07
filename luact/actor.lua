-- external modules
local uuid = require 'luact.uuid'
uuid.DEBUG = true
local vid = require 'luact.vid'
local dht = require 'luact.dht'
local conn = require 'luact.conn'

local exception = require 'pulpo.exception'

local _M = {}
_M.sys_event = {
	DESTROY = "destroy",
	LINK_DEAD = "link_dead",
	LINK = "link",
	UNLINK = "unlink",
}

exception.define('actor_not_found')
exception.define('actor_body_not_found')
exception.define('actor_method_not_found')
exception.define('actor_runtime_error')


-- local function
--[[
	manages relation of actor
--]]
local actormap = {}
local actor_index = {}
local actor_mt = {
	__index = actor_index
}
function actor_index.new(id, event_handler)
	-- TODO : allocate from cache if available.
	return setmetatable({
		uuid = id,
		links = {},
		event = event_handler,
	}, actor_mt)
end
function actor_index:destroy(reason)
 	for i=#self.links,1,-1 do
		local link = table.remove(self.links, i)
		link:notify___sys_event__(_M.sys_event.LINK_DEAD, self.uuid, reason) -- do not wait response
	end
	-- TODO : cache this (to reduce GC)
end
-- called with actor:__sys__event
function try_to_hook_sys_event(body, event, ...)
	local ok, r = pcall(body.__sys_event__, body, event, ...)
	return ok and r
end
function actor_index:event__(body, event, ...)
	logger.info('sys_event', self.uuid, body, event, ...)
	-- TODO : test and if that is significantly slow, better way to hook system events.
	if try_to_hook_sys_event(body, event, ...) then
		return
	end
	if event == _M.sys_event.LINK_DEAD or event == _M.sys_event.DESTROY then
		-- by default, if linked actor dead, you also dies.
		_M.destroy(self.uuid, "sys_event")
	elseif event == _M.sys_event.LINK then
		table.insert(self.links, ({...})[1])
	elseif event == _M.sys_event.UNLINK then
		local unlinked = ({...})[1]
		for idx,link in ipairs(self.links) do
			if link == unlinked then
				table.remove(self.link, idx)
			end
		end
	end
end

--[[
	make uuid cdata itself message-sendable
--]]
local prefixes = {
	notify_ 	= 0x00000001,
	timed_ 		= 0x00000002,
	async_		= 0x00000004, -- TODO
	__sys_ 		= 0x00000008,
}
_M.prefixes = prefixes
local patterns = {}
for k,v in pairs(prefixes) do
	patterns[k] = "(.-)"..k.."(.*)"
end
local function parse_method_name(name)
	local flag, index, match = 0, 0
	repeat
		match = false
		for k,v in pairs(prefixes) do
			if bit.band(flag, v) == 0 then
				local pattern = patterns[k]
				local tmp = name:gsub(pattern, "%1%2", 1)
				if #tmp ~= #name then
					name = tmp
					flag = bit.bor(flag, v)
					match = true
				end
			end
		end
	until not match 
	return name,flag
end	

local function uuid_caller_proc(t, ...)
	local c = conn.get(t.uuid)
	return c:dispatch(t, ...)
end
local uuid_caller_mt = {
	__call = uuid_caller_proc,
}
local methods_cache = {}
local uuid_metatable = {
	__index = function (t, k)
		-- print('uuid__index:', k)
		local methods = rawget(methods_cache, t:__serial())
		if not methods then
			methods = {}
			rawset(methods_cache, t:__serial(), methods)
		end
		local v = rawget(methods, k)
		if not v then
			local name, flag = parse_method_name(k)
			-- TODO : cache this?
			-- TODO : need to compare speed with closure generation
			v = setmetatable({method = name, flag = flag, uuid = t}, uuid_caller_mt)
			rawset(methods, k, v)
		end
		return v
	end,
	__gc = function (t)
		-- print('uuid_gc:', t)
		rawset(methods_cache, t:__serial(), nil)
		uuid.free(t)
	end,
}

--[[
	make vid cdata itself message-sendable
--]]
local function vid_caller_proc(t, ...)
	local c = conn.get_by_hostname(t.vid.host)
	return c:vdispatch(t, ...)
end
local vid_caller_mt = {
	__call = vid_caller_proc,
}
local vid_metatable = {
	__index = function (t, k)
		local name, flag = parse_method_name(k)
		-- TODO : cache this?
		-- TODO : need to compare speed with closure generation
		-- TODO : seems to be directly related with sender variations, like conn:send, conn:call, conn:sys, ... 
		v = setmetatable({method = name, flag = flag, vid = t}, vid_caller_mt)
		rawset(t, k, v)
		return v
	end,	
}

-- vars
local bodymap = {}

-- module function
function _M.initialize(cmdl_args)
	uuid.initialize(uuid_metatable, cmdl_args.startup_at, cmdl_args.local_address)
	vid.initialize(vid_metatable)
	_M.root = _M.new(function ()
		return {
			new = _M.new,
			new_link = _M.new_link,
			register = _M.register,
		}
	end)
end

--[[
	actor creation/deletion
--]]
function _M.new(ctor, ...)
	return _M.new_link_with_id(nil, nil, ctor, ...)
end
function _M.new_link(to, ctor, ...)
	return _M.new_link_with_id(to, nil, ctor, ...)
end
function _M.new_link_with_id(to, id, ctor, ...)
	id = id or uuid.new()
	local s = id:__serial()
	if to then to:sys_event(_M.sys_event.LINK, id) end
	local body = ctor(...)
	local a = actor_index.new(id)
	actormap[body] = a
	bodymap[s] = body
	if _M.debug then
		logger.notice('add bodymap', s, body, debug.traceback())
	end
	return id
end
function _M.register(name, ctor, ...)
	-- TODO : choose owner thread of this actor
	-- maybe using consistent hash with parameter 'name' and uuid of root actor (_M.root)
	-- if name should be created in this node then
		local a = _M.new(ctor, ...)
		-- TODO : register name and uuid to dht
		return a
	--[[ else
		local assigned_node = dht.get_node_for(name)
		return assigned_node.register(name, ctor, ...)	
	end ]]--
end
function _M.unregister(vid)
end
--[[
	create remote actor reference from its name
	eg. ssh+json://myservice.com:10000/user/id0001
--]]
function _M.ref(name)
	return vid.new(name)
end

local function destroy_by_serial(s, reason)
	local b = bodymap[s]
	-- body only exists the node which create it.
	if b then
		bodymap[s] = nil
		local a = actormap[b]
		if a then
			a:destroy(reason)
			actormap[b] = nil
		end
		if b.__destroy__ then
			b:__destroy__(reason)
		end
	end
end
local function safe_destroy_by_serial(s, reason)
	local ok, r = pcall(destroy_by_serial, s, reason) 
	if not ok then logger.error('destroy fails:'..tostring(r)) end
end
function _M.destroy(id, reason)
	local ok, r = pcall(destroy_by_serial, id:__serial(), reason)
	if not ok then logger.error('destroy fails:'..tostring(r)) end
end
local function body_of(serial)
	return bodymap[serial]
end
function _M.id_of(object)
	return actormap[object].uuid
end
_M.proxy_of = _M.id_of
function _M.dispatch_send(local_id, method, ...)
	local s = uuid.serial_from_local_id(local_id)
	local b = body_of(s)
	if not b then return false, exception.new('actor_body_not_found', tostring(uuid.from_local_id(local_id))) end
	local r = {pcall(b[method], b, ...)}
	if not r[1] then 
		if not b[method] then 
			r[2] = exception.new('actor_method_not_found', tostring(uuid.from_local_id(local_id)), method)
		else
			r[2] = exception.new('actor_runtime_error', tostring(uuid.from_local_id(local_id)), r[2])
		end
		safe_destroy_by_serial(s, r[2]) 
	end
	return unpack(r)
end
function _M.dispatch_call(local_id, method, ...)
	local s = uuid.serial_from_local_id(local_id)
	local b = body_of(s)
	if not b then return false, exception.new('actor_body_not_found', tostring(uuid.from_local_id(local_id))) end
	local r = {pcall(b[method], ...)}
	if not r[1] then 
		if not b[method] then 
			r[2] = exception.new('actor_method_not_found', tostring(uuid.from_local_id(local_id)), method)
		else
			r[2] = exception.new('actor_runtime_error', tostring(uuid.from_local_id(local_id)), r[2])
		end
		safe_destroy_by_serial(s, r[2]) 
	end
	return unpack(r)
end
function _M.dispatch_sys(local_id, method, ...)
	local s = uuid.serial_from_local_id(local_id)
	local b = body_of(s)
	local p = actormap[b]
	if not p then return false, exception.new('actor_not_found', tostring(uuid.from_local_id(local_id))) end
	local r = {pcall(p[method], p, b, ...)}
	if not r[1] then 
		if not b then 
			r[2] = exception.new('actor_body_not_found', tostring(uuid.from_local_id(local_id)))
		elseif not b[method] then 
			r[2] = exception.new('actor_method_not_found', tostring(uuid.from_local_id(local_id)), method)
		else
			r[2] = exception.new('actor_runtime_error', tostring(uuid.from_local_id(local_id)), r[2])
		end
		safe_destroy_by_serial(s, r[2]) 
	end
	return unpack(r)
end

return _M
