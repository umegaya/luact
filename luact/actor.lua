-- external modules
local uuid = require 'luact.uuid'
local vid = require 'luact.vid'
local conn -- luact.conn
local dht = require 'luact.dht'
local exception = require 'pulpo.exception'

-- local function
--[[
	manages relation of actor
--]]
local proxy = {}
function proxy.new()
	return setmetatable({
		links = {}
	}, proxy)
end
function proxy.cleanup(p, reason)
 	for i=#p.links,1,-1 do
		local link = table.remove(p.links, i)
		link:notify_sys_event('destroy', id, reason)
	end
end

--[[
	make uuid cdata itself message-sendable
--]]
local prefixes = {
	async_ 		= 0x00000001,
	timed_ 		= 0x00000002,
	mcast_ 		= 0x00000004,
	protected_ 	= 0x00000008,
}
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
	if name[1] == '_' then
		flag = bit.bor(flag, prefixes.protected_)
	end
	return name,flag
end	
local function uuid_caller_proc(t, ...)
	local c = conn.get(t.__uuid)
	return c:send(t.uuid:__serial(), t.method, t.flag, ...)
end
local uuid_caller_mt = {
	__call = uuid_caller_proc,
}
local methods_cache = {}
local uuid_metatable = {
	__index = function (t, k)
		local methods = rawget(methods_cache, t:__serial())
		if not methods then
			rawset(methods_cache, t:__serial(), {})
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
		rawset(methods_cache, t:__serial(), nil)
		uuid.free(t)
	end,
}

--[[
	make vid cdata itself message-sendable
--]]
local function vid_caller_proc(t, ...)
	local c = conn.get_by_url(t.vid[1])
	return c:send(t.vid[2], t.__method, t.__flag, ...)
end
local vid_caller_mt = {
	__call = vid_caller_proc,
}
local vid_metatable = {
	__index = function (t, k)
		local name, flag = parse_method_name(k)
		-- TODO : cache this?
		-- TODO : need to compare speed with closure generation
		v = setmetatable({method = name, flag = flag, vid = t}, vid_caller_mt)
		rawset(t, k, v)
		return v
	end,	
}

-- vars
local _M = {}
local bodies, proxies = {}, {}

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
	conn = require 'luact.conn'
end

--[[
	current system events are:
	'destroy' (actor, reason) : *actor* died by *reason*
	'link' (actor) : please link *actor* to receiver
--]]
function _M.default_sys_event(receiver, event, ...)
	logger.info('sysevent', receiver, event, ...)
	if event == 'destroy' then
		_M.destroy(receiver)
	elseif event == 'link' then
		table.insert(proxies[receiver.serial].link, select(1, ...))
	end
end

--[[
	supply least necessary method or property to object.
--]]
local function actorize(uuid, object)
	object.__actor = uuid
	object.sys_event = object.sys_event or _M.default_sys_event
end

--[[
	actor creation/deletion
--]]
function _M.new(ctor, ...)
	return _M.new_link(nil, nil, ctor, ...)
end
function _M.new_link(to, ctor, ...)
	return _M.new_link(to, nil, ctor, ...)
end
function _M.new_link_with_id(to, id, ctor, ...)
	id = id or uuid.new()
	local s = id:__serial()
	if to then to:sys_event('link', id) end
	if not proxies[s] then
		proxies[s] = proxy.new()
	end
	bodies[s] = actorize(id, ctor(...))
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
--[[
	create remote actor reference from its name
	eg. ssh+json-rpc://myservice.com:10000/user/id0001
--]]
function _M.ref(name)
	return vid.new(name)
end

function _M.destroy(id, reason)
	local s = id:__serial()
	local p = proxies[s]
	if p then
		local a = bodies[s]
		-- body only exists the node which create it.
		if a then
			bodies[s] = nil
			if a.destroy then
				a:destroy(reason)
			end
		end
		p:cleanup(reason)
	end
end
function _M.body_of(id)
	return bodies[id:__serial()]
end

return _M

