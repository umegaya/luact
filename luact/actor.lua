-- external modules
local uuid = require 'luact.uuid'
uuid.DEBUG = true
local vid = require 'luact.vid'
local dht = require 'luact.dht'
local conn = require 'luact.conn'
local clock = require 'luact.clock'

local util = require 'pulpo.util'

local _M = {}
_M.EVENT_DESTROY = "destroy"
_M.EVENT_LINK_DEAD = "link_dead"
_M.EVENT_LINK = "link"
_M.EVENT_UNLINK = "unlink"

-- exceptions
local exception = require 'pulpo.exception'
exception.define('actor_not_found')
exception.define('actor_no_body')
exception.define('actor_no_method')
exception.define('actor_error')
exception.define('actor_runtime_error')
exception.define('actor_timeout')
exception.define('actor_temporary_fail')


-- local function
--[[
	manages relation of actor
--]]
-- TODO : sometimes cdata is resulting different table key.
-- eg) refer member cdata (even if it is pointer!!)
-- use FFI implementation of map
local actormap = {}
local actor_index = {}
local actor_mt = {
	__index = actor_index
}
function actor_index.new(id, opts)
	-- TODO : allocate from cache if available.
	return setmetatable({
		uuid = id,
		links = {},
		supervised = opts.supervised,
	}, actor_mt)
end
function actor_index:destroy(reason)
 	for i=#self.links,1,-1 do
		local link = table.remove(self.links, i)
		link:notify___actor_event__(_M.EVENT_LINK_DEAD, self.uuid, reason) -- do not wait response
 		-- print('send linkdead to:', link, self, #self.links)
	end
	self.supervised = nil
	-- TODO : cache this (to reduce GC)
end
function actor_index:unlink(unlinked)
	for idx,link in ipairs(self.links) do
		if uuid.equals(link, unlinked) then
		-- if link == unlinked then
			table.remove(self.links, idx)
			return
		end
	end
end
-- called with actor:__actor_event__()
function actor_index:event__(body, event, ...)
	logger.info('actor_event', self.uuid, body, event, ...)
	-- TODO : test and if that is significantly slow, better way to hook system events.
	if body.__actor_event__ and body:__actor_event__(self, event, ...) then
		return
	end
	if event == _M.EVENT_LINK_DEAD then
		-- by default, if linked actor dead, you also dies.
		_M.destroy(self.uuid)
	elseif event == _M.EVENT_DESTROY then
		_M.destroy(self.uuid)
	elseif event == _M.EVENT_LINK then
		table.insert(self.links, ({...})[1])
		-- print('add link:', ({...})[1], self, #self.links)
	elseif event == _M.EVENT_UNLINK then
		self:unlink(({...})[1])
	end
end

--[[
	make uuid cdata itself message-sendable
--]]
local prefixes = {
	notify_ 	= 0x00000001,
	timed_ 		= 0x00000002,
	async_		= 0x00000004, -- TODO
	__actor_ 	= 0x00000008,
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
	local c = conn.get(t.id)
	return c:dispatch(t, ...)
end
local uuid_caller_mt = {
	__call = uuid_caller_proc,
}
local methods_cache = {}
local uuid_metatable = {
	__index = function (t, k)
		-- print('uuid__index:', k)
		local v = rawget(methods_cache, k)
		if v then
			if v.id then -- cache exist but in-use
				-- copy on write
				v = setmetatable(util.copy_table(v), uuid_caller_mt)
				rawset(methods_cache, k, v)
			end
		else -- cache not exist
			local name, flag = parse_method_name(k)
			v = setmetatable({method = name, flag = flag}, uuid_caller_mt)
			rawset(methods_cache, k, v)
		end
		v.id = t
		return v
	end,
	__gc = function (t)
		-- print('uuid_gc:', t)
		uuid.free(t)
	end,
}

--[[
	make vid cdata itself message-sendable
--]]
local function vid_caller_proc(t, ...)
	local c = conn.get_by_hostname(t.id.host)
	return c:vdispatch(t, ...)
end
local vid_caller_mt = {
	__call = vid_caller_proc,
}
local vid_metatable = {
	__index = function (t, k)
		local v = rawget(methods_cache, k)
		-- cache not exist or in-use
		if v then
			if v.id then -- cache exist but in-use
				-- copy on write
				v = setmetatable(util.copy_table(v), vid_caller_mt)
				rawset(methods_cache, k, v)
			end
		else -- cache not exist
			local name, flag = parse_method_name(k)
			v = setmetatable({method = name, flag = flag}, vid_caller_mt)
			rawset(methods_cache, k, v)
		end
		v.id = t
		return v
	end,	
}

-- vars
local bodymap = {}

-- module function
local root_actor_id
function _M.initialize(opts)
	uuid.initialize(uuid_metatable, opts.startup_at, opts.local_address)
	vid.initialize(vid_metatable)
	root_actor_id = uuid.first()
end

--[[
	actor creation/deletion
--]]
local default_opts = {}
function _M.new(ctor, ...)
	return _M.new_link_with_opts(nil, default_opts, ctor, ...)
end
function _M.new_root(ctor, ...)
	local s = uuid.serial(root_actor_id)
	if bodymap[s] then
		return actormap[bodymap[s]]
	end
	return _M.new_link_with_opts(nil, { uuid = root_actor_id }, ctor, ...)
end
function _M.new_link(to, ctor, ...)
	return _M.new_link_with_opts(to, default_opts, ctor, ...)
end
function _M.new_link_with_opts(to, opts, ctor, ...)
	local body = ctor(...)
	local id = opts.uuid or uuid.new()
	local s = uuid.serial(id)
	if to then 
		local ok, r = pcall(to.__actor_event__, to, _M.EVENT_LINK, id)
		if not ok then
		 	if body.__actor_destroy__ then body:__actor_destroy__(r) end
			error(r)
		end
	end
	local a = actor_index.new(id, opts)
	table.insert(a.links, to)
	-- print('entry actormap', actormap, body, a)
	actormap[body] = a
	if type(body) ~= 'table' and type(body) ~= 'cdata' then
		exception.raise('invalid', 'wrong type of actor body', type(body))
	end
	bodymap[s] = body
	if _M.debug then
		logger.notice('add bodymap', s, body, debug.traceback())
	end
	if type(body) == 'cdata' then
		if (require 'reflect').typeof(body).name == 'luact_uuid' then
			exception.raise('invalid', 'actor is specified as body at:'..debug.traceback())
		end
	end
	return id
end
function _M.monitor(watcher_actor, target_actor)
	local w, t = _M.of(watcher_actor), _M.of(target_actor)
	if not w then exception.raise('not_found', 'watcher_actor') end
	if not t then exception.raise('not_found', 'target_actor') end
	table.insert(t.links, w.uuid)
end
function _M.root_of(machine_id, thread_id)
	assert(thread_id, "thread_id should")
	return uuid.first(machine_id or uuid.node_address, thread_id)
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
	create remote actor reference from its url
	eg. ssh+json://myservice.com:10000/user/id0001
--]]
function _M.ref(url)
	return vid.new(url)
end

local ACTOR_WAIT_RESTART = false
local function destroy_by_serial(s, reason)
	local b = bodymap[s]
	-- body only exists the node which create it.
	if b then
		bodymap[s] = nil
		local a = actormap[b]
		if a then
			-- if no reason, force destroy and superviser not works.
			if reason and a.supervised then
				-- if restart not finished so long time, at least error causes in supervisor:restart_child.
				-- then actor._set_restart_result calls. it sets bodymap[s] to nil to reset state
				bodymap[s] = ACTOR_WAIT_RESTART
			end
			a:destroy(reason)
			actormap[b] = nil
		end
		if b.__actor_destroy__ then
			b:__actor_destroy__(reason)
		end
		logger.warn('actor destroyed by', reason or "system")
	end
end
local function safe_destroy_by_serial(s, reason)
	local ok, r = pcall(destroy_by_serial, s, reason) 
	if not ok then logger.error('destroy fails:'..tostring(r).." reason: "..tostring(reason)) end
end
local function body_of(serial)
	return bodymap[serial]
end

function _M.alive(id)
	return body_of(uuid.serial(id))
end
function _M.destroy(id, reason)
	local ok, r = pcall(destroy_by_serial, uuid.serial(id), reason)
	if not ok then logger.error('destroy fails:'..tostring(r)) end
end
function _M.of(object)
	-- print('get actor from body', object, actormap[object])
	local a = actormap[object]
	return a and a.uuid or nil
end
-- should use only from internal.
function _M._set_restart_result(id, result)
	local s = uuid.serial(id)
	if result then
		assert(bodymap[s], exception.new('actor_not_found', id))
	elseif bodymap[s] == ACTOR_WAIT_RESTART then
		bodymap[s] = nil -- restart fails, so remove wait restart state.
	end
end
local function err_handler(e)
	if type(e) == 'table' and e.is then
		e:set_bt()
	else
		e = exception.new_with_bt('actor_error', debug.traceback(), e)
	end
	return e
end
function _M.is_fatal_error(e)
	return not (e:is('runtime') or e:is('actor_runtime_error') or e:is('actor_timeout'))
end
function _M.dispatch_send(local_id, method, ...)
	local s = uuid.serial_from_local_id(local_id)
	local b = body_of(s)
	if not b then 
		local tp = (b ~= ACTOR_WAIT_RESTART and 'actor_no_body' or 'actor_temporary_fail')
		return false, exception.new(tp, tostring(uuid.from_local_id(local_id))) 
	end
	local r = {xpcall(b[method], err_handler, b, ...)}
	if not r[1] then 
		if not b[method] then 
			r[2] = exception.new('actor_no_method', tostring(uuid.from_local_id(local_id)), method)
		elseif _M.is_fatal_error(r[2]) then
			logger.warn('fatal message error at', uuid.from_local_id(local_id), method, 'by', r[2])
			safe_destroy_by_serial(s, r[2]) 
		end
	end
	return unpack(r)
end
function _M.dispatch_call(local_id, method, ...)
	local s = uuid.serial_from_local_id(local_id)
	local b = body_of(s)
	if not b then 
		local tp = (b ~= ACTOR_WAIT_RESTART and 'actor_no_body' or 'actor_temporary_fail')
		return false, exception.new(tp, tostring(uuid.from_local_id(local_id))) 
	end
	local r = {xpcall(b[method], err_handler, ...)}
	if not r[1] then 
		if not b[method] then 
			r[2] = exception.new('actor_no_method', tostring(uuid.from_local_id(local_id)), method)
		elseif _M.is_fatal_error(r[2]) then
			logger.warn('fatal message error at', uuid.from_local_id(local_id), method, 'by', r[2])
			safe_destroy_by_serial(s, r[2]) 
		end
	end
	return unpack(r)
end
function _M.dispatch_sys(local_id, method, ...)
	local s = uuid.serial_from_local_id(local_id)
	local b = body_of(s)
	local p = actormap[b]
	if not p then return false, exception.new('actor_not_found', tostring(uuid.from_local_id(local_id))) end
	local r = {xpcall(p[method], err_handler, p, b, ...)}
	if not r[1] then 
		if not b then 
			local tp = (b ~= ACTOR_WAIT_RESTART and 'actor_no_body' or 'actor_temporary_fail')
			return false, exception.new(tp, tostring(uuid.from_local_id(local_id))) 
		elseif not p[method] then 
			r[2] = exception.new('not_found', p, method)
		elseif _M.is_fatal_error(r[2]) then
			logger.warn('fatal message error at', uuid.from_local_id(local_id), method, 'by', r[2])
			safe_destroy_by_serial(s, r[2]) 
		end
	end
	return unpack(r)
end

return _M
