-- external modules
local memory = require 'lib.pulpo.memory'
local uuid = require 'luact.uuid'

local mqueue = require 'luact.mqueue'
local session = require 'luact.session'
local serde = require 'luact.serde'

-- local function
flags = {
	ASYNC 		= 0x00000001,
	TIMED 		= 0x00000002,
	MCAST 		= 0x00000004,
	PROTECTED 	= 0x00000008,
}

local function parse_opts(name)
	local flag, index, match = 0, 0
	repeat
		match = false
		for k,v in pairs(prefixes) do
			if bit.band(flag, prefixes[k]) == 0 then
				local pattern = "(.-)"..k.."(.*)"
				local tmp = string.gsub(name, pattern, "%1%2", 1)
				if #tmp ~= #name then
					name = tmp
					flag = bit.bor(flag, v)
					match = true
				end
			end
		end
	until not match 
	if string.find(name, '_') == 1 then
		flag = bit.bor(flag, constant.flags.PROTECTED)
	end
	return name,flag
end	

create_luact_table = function (src, key)
	return setmetatable({__src = src, __key = key}, {
		__call = function (t, ...)
			local src = t.__src
			local s = session.find_or_new(src:__addr())
			if select(1, ...) == t then -- a:b(...) style
				return s:send(src:__short_id(), t.__key, ...)
			else -- a.b(...) style
				return s:call(src:__short_id(), t.__key, ...)
			end
		end,
		__index = function (t, k)
			local opts,name = parse_opt(k)
			local v = t.__key and
				create_luact_table(t.__src, t.__key.."."..name, opts) or
				create_luact_table(t.__src, name, opts)
			rawset(t, k, v)
			return v
		end,
	})
end

--===================================================================================
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


--===================================================================================
-- module body
--===================================================================================
-- local functions
--[[
	system event is:
	'destroy' (actor, reason) : *actor* died by *reason*
	'link' (actor) : please link *actor* to receiver
]]
local function default_sys_event(receiver, event, ...)
	logger.info('sysevent', receiver, event, ...)
	if event == 'destroy' then
		_M.destroy(receiver)
	elseif event == 'link' then
		table.insert(proxies[receiver.serial].link, select(1, ...))
	end
end

--[[
	supply least necessary method or property to object.
]]
local function actorize(uuid, object)
	object.__actor = uuid
	object.sys_event = object.sys_event or default_sys_event
end

--[[
	supply least necessary method or property to object.
]]
local function create_metatable()
end

--[[
	create root actor which can create actor.
]]
local function root_actor()
	return {
		new = _M.new,
	}
end

-- vars
local _M = {}
local bodies = {}

-- module function
function _M.initialize(opts)
	uuid.initialize(create_metatable(), opts.args.startup_at, opts.args.local_address)
	_M.root = _M.new(root_actor)
end

function _M.new(ctor, ...)
	local id = uuid.new()
	local s = id.serial
	bodies[s] = actorize(id, ctor(...))
	return id
end

function _M.destroy(id, reason)
	local s = id.serial
	local a = bodies[s]
	-- body only exists the node which create it.
	if a then
		bodies[s] = nil
		if a.destroy then
			a:destroy(reason)
		end
		uuid.free(id)
	end
end

return _M

