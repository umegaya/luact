local exlib = require 'luact.exlib'

local pulpo = require 'pulpo.init'
local exception = require 'pulpo.exception'
local pulpo_package = require 'pulpo.package'
local util = require 'pulpo.util'
local fs = require 'pulpo.fs'
-- pulpo_package.DEBUG= true
local ffi = require 'ffiex.init'

local serpent = require 'serpent'

require 'luact.util.patch'
local actor = require 'luact.actor'
local optparse = require 'luact.optparse'
local listener = require 'luact.listener'
local pbuf = require 'luact.pbuf'
local router = require 'luact.router'
local conn = require 'luact.conn'
local supervise = require 'luact.supervise'
local uuid = require 'luact.uuid'
local vid = require 'luact.vid'
local peer = require 'luact.peer'
local common = require 'luact.serde.common'

local _M = {}
_M.event = require 'pulpo.event'
_M.tentacle = require 'pulpo.tentacle'
_M.clock = require 'luact.clock'
_M.memory = require 'pulpo.memory'
_M.exception = require 'pulpo.exception'
_M.util = require 'pulpo.util'
_M.listen = listener.listen
_M.thread_id = false
_M.machine_id = false


_M.DEFAULT_ROOT_DIR = fs.abspath("tmp", "luact")

-- command line option definitions
local opts_defs = {
	{"a", "local_address"},
	{"p", "parent_address"},
	{"t", "startup_at"},
	{nil, "port"},
	{nil, "n_core", "%w+", function (m)
		if m == "false" then return false end 
		return tonumber(m)
	end}, 
	{nil, "timeout_resolution"},
	{nil, "proto"},
}

-- local function
local factory 
factory = {
	["table"] = function (tbl)
		return tbl
	end,
	["cdata"] = function (cdata)
		return cdata
	end,
	["function"] = function (fn, ...)
		return fn(...)
	end,
	["string"] = function (str, ...)
		if str:find("/") or str:match("%.lua$") then
			return factory["file"](str, ...)
		else
			return factory["module"](str, ...)
		end
	end,
	["file"] = function (file)
		local ok, err = loadfile(file)
		if err then
			exception.raise('runtime', err)
		end
		return ok()
	end,
	["module"] = function (mod, fallback)
		local ok, r = pcall(require, mod)
		if not ok then
			logger.warn('require module fails', r)
			if fallback then
				if os.execute('luarocks install '..fallback) ~= 0 then
					exception.raise('runtime', 'os.execute')
				end
				ok, r = pcall(require, mod)
			end
		end
		if not ok then
			exception.raise('runtime', err)
		end
		return r
	end,
}
local from_file, from_module = factory["file"], factory["module"]

-- additional thread startup routines
local function init_worker(opts)
	opts = assert(loadstring(opts))()
	local _luact = require 'luact.init'
	_luact.initialize(opts)
end
local function init_worker_and_global_ref(opts)
	opts = assert(loadstring(opts))()
	_G.luact = require 'luact.init'
	_G.luact.initialize(opts)
end
local function get_opts(opts)
	opts = util.merge_table(require 'luact.option', opts or {})
	local cmdl, others = optparse(_G.arg, opts_defs)
	for i=1,#others do
		if others[i]:match("^.+%.lua$") then
			opts.executable = others[i]
			break
		end
	end
	return util.merge_table(opts, cmdl, true)
end


-- module function 
function _M.start(opts, executable)
	opts = get_opts(opts)
	opts.init_params = serpent.dump(opts)
	opts.init_proc = _G.luact and init_worker_and_global_ref or init_worker
	pulpo.initialize(opts)
	-- TODO : need to change pulpo configuration from commandline
	_M.initialize(opts)
	pulpo.run(opts, executable or opts.executable)
end
function _M.stop()
	pulpo.stop()
end
function _M.initialize(opts)
	-- initialize deferred modules in luact
	pulpo_package.init_modules(exlib.LUACT_BUFFER, exlib.LUACT_IO)
	-- initialize other modules
	uuid.initialize(actor.uuid_metatable, opts.startup_at, opts.local_address)
	actor.initialize(opts.actor)
	if opts.dht.gossip_port then
		vid.initialize_dist(actor.vid_metatable, opts.parent_address, opts.dht, opts.vid)
	else
		vid.initialize_local(actor.vid_metatable, opts.vid)
	end
	_M.dht = vid.dht
	conn.initialize(opts.conn)
	router.initialize(opts.router)

	-- initialize node identifier
	_M.thread_id = pulpo.thread_id
	_M.machine_id = uuid.node_address
	_M.n_core = opts.n_core or util.n_cpu()
	_M.verbose = pulpo.verbose
	logger.notice('node_id', ('%x:%u'):format(_M.machine_id, _M.thread_id))

	-- initialize listener of internal actor messaging 
	listener.unprotected_listen(tostring(opts.conn.internal_proto).."://0.0.0.0:"..tostring(opts.conn.internal_port))
	-- external port should be declared at each startup routine.
	-- create initial root actor, which can be accessed only need to know its hostname.
	_M.root_actor = actor.new_root(function (options)
		local arbiter_opts = options.arbiter
		local gossiper_opts = options.gossiper
		local arbiter_module = require('luact.cluster.'..(arbiter_opts.kind or 'raft'))
		local gossiper_module = require('luact.cluster.'..(gossiper_opts.kind or "gossip"))
		if pulpo.thread_id ~= 1 then
			table.insert(gossiper_opts.config.nodelist, actor.root_of(nil, 1, true))
			gossiper_opts.config.local_mode = true
		end
		_M.root = {
			arbiter = function (group, fsm_factory, opts, ...)
				return fsm_factory and arbiter_module.new(group, fsm_factory, util.merge_table(arbiter_opts.config, opts or {}), ...)
					or arbiter_module.find(group)
			end,
			gossiper = function (port, opts)
				return gossiper_module.new(port or options.conn.internal_port, util.merge_table(gossiper_opts.config, opts or {}))
			end,
			stat = function ()
				-- TODO : add default stats functions and refine customize way.
				local ret = {}
				return util.merge_table(ret, options.stat_proc and options.stat_proc() or {})
			end,
			push = function (id, cmd, ...)
				local c = conn.get_by_peer_id(id)
				if c then
					if bit.band(cmd, router.NOTICE_MASK) ~= 0 then
						c:rawsend(cmd, ...)
					else
						return c:send_and_wait(cmd, ...)
					end
				else
					return false, exception.new('actor_not_found', 'peer', id)
				end
			end,
			["require"] = _M.require,
			["load"] = _M.load,
		}
		return _M.root
	end, opts)
end
function _M.load(file, opts)
	return actor.new(from_file, file, opts)
end
function _M.require(module, opts)
	return actor.new(from_module, module, opts)
end
function _M.supervise(target, opts, ...)
	return supervise(assert(factory[type(target)]), opts, target, ...)
end

function _M.error(...)
	exception.raise('actor_error', ...)
end

function _M.register(vid, fn_or_opts, fn_or_args1, ...)
	if type(fn_or_opts) == 'table' then
		return _M.dht:put(vid, fn_or_opts.multi_actor, function (ctor, opts, ...)
			return _M.supervise(ctor, opts, ...)
		end, fn_or_args1, fn_or_opts.supervise, ...)
	else
		return _M.dht:put(vid, false, function (ctor, opts, ...)
			return _M.supervise(ctor, opts, ...)
		end, fn_or_opts, false, fn_or_args1, ...)
	end
end
function _M.unregister(vid, actor_id)
	_M.dht:remove(vid, actor_id, function (key, id)
		local tid = uuid.thread_id(id)
		if tid == _M.thread_id then
			actor.destroy(id)
		else
			actor.root_of(uuid.address(id), tid).notify_unregister(id)
		end
	end)
end
--[[
	create remote actor reference from its url
	eg. ssh+json://myservice.com:10000/user/id0001
--]]
function _M.ref(url)
	return vid.new(url)
end
--[[
	returns remote reference to invoke current coroutine execution
	if calling peer and it returns actor_not_found, this means peer is gone (closed)
--]]
function _M.peer(dest_path)
	if not dest_path then exception.raise('invalid', 'peer require destination path') end
	local id = _M.tentacle.get_context()[router.CONTEXT_PEER_ID]
	return id and peer.new(id, dest_path)
end
function _M.monitor(watcher, target)
	actor.monitor(watcher, target)
end
function _M.kill(...)
	for _,act in ipairs({...}) do
		act:__actor_event__(actor.EVENT_DESTROY)
	end
end
_M.of = actor.of
_M.root_of = actor.root_of
-- for calling from dockerfile. initialize cdef cache of ffiex 
-- so that it can be run 
function _M.init_cdef_cache()
	(require 'ffiex.init').init_cdef_cache()
end
return setmetatable(_M, {
	__call = function (t, target, ...)
		return actor.new(assert(factory[type(target)]), target, ...)
	end
})
