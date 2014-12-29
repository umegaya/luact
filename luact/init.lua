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
local dht = require 'luact.dht'
local pbuf = require 'luact.pbuf'
local router = require 'luact.router'
local conn = require 'luact.conn'
local supervise = require 'luact.supervise'

local _M = {}

_M.DEFAULT_ROOT_DIR = fs.abspath("tmp", "luact")

-- command line option definitions
local opts_defs = {
	{"a", "local_address"},
	{"t", "startup_at"},
	{"p", "port"},
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


-- module function 
function _M.start(opts, executable)
	opts.init_params = serpent.dump(opts)
	opts.init_proc = _G.luact and init_worker or init_worker_and_global_ref
	pulpo.initialize(opts)
	-- TODO : need to change pulpo configuration from commandline
	_M.initialize(opts)
	pulpo.run(opts, executable)
end
function _M.stop()
	pulpo.stop()
end
function _M.initialize(opts)
	opts = util.merge_table(require 'luact.option', opts or {})
	util.merge_table(opts, optparse(_G.arg, opts_defs), true)
	-- initialize deferred modules in luact
	pulpo_package.init_modules(exlib.LUACT_BUFFER, exlib.LUACT_IO)
	-- initialize other modules
	actor.initialize(opts.actor)
	conn.initialize(opts.conn)
	router.initialize(opts.router)
	dht.initialize(opts.dht)

	-- initialize listener of internal actor messaging 
	listener.unprotected_listen(tostring(opts.conn.internal_proto).."://0.0.0.0:"..tostring(opts.conn.internal_port))
	-- external port should be declared at each startup routine.
	-- because it is likely to open multiple listener port.

	-- create initial root actor, which can be accessed only need to know its hostname.
	_M.root_actor = actor.new_root(function (options)
		local arbiter_opts = options.arbiter or {}
		local arbiter_module = (arbiter_opts ~= false and require('luact.cluster.'..(arbiter_opts.kind or 'raft')))
		_M.root = {
			new = actor.new,
			destroy = actor.destroy,
			register = actor.register,
			unregister = actor.unregister, 
			["require"] = _M.require, 
			load = _M.load, 
			arbiter = function (group, fsm_factory, opts, ...)
				if not arbiter_module then
					exception.raise('invalid', 'config', 'this node not using arbiter')
				end
				if fsm_factory then 
					return arbiter_module.new(group, fsm_factory, opts or arbiter_opts.config, ...)
				else
					return arbiter_module.find(group)
				end
			end,
			stat = function (self)
				-- TODO : add default stats functions and refine customize way.
				local ret = {}
				return util.merge_table(ret, options.stat_proc and options.stat_proc() or {})
			end,
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
function _M.monitor(watcher, target)
	actor.monitor(watcher, target)
end
function _M.kill(...)
	for _,act in ipairs({...}) do
		act:__actor_event__(actor.EVENT_DESTROY)
	end
end
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
