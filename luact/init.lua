local exlib = require 'luact.exlib'

local pulpo = require 'pulpo.init'
local exception = require 'pulpo.exception'
local pulpo_package = require 'pulpo.package'
local util = require 'pulpo.util'
-- pulpo_package.DEBUG= true
local ffi = require 'ffiex.init'

local actor = require 'luact.actor'
local optparse = require 'luact.optparse'
local listener = require 'luact.listener'
local dht = require 'luact.dht'
local pbuf = require 'luact.pbuf'
local router = require 'luact.router'
local conn = require 'luact.conn'
local supervise = require 'luact.supervise'

local _M = {}

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
	print('tbl:', tbl)
		return tbl
	end,
	["cdata"] = function (cdata)
		return cdata
	end,
	["function"] = function (fn)
		return fn
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
local function init_worker()
	local _luact = require 'luact.init'
	_luact.initialize()
end
local function init_worker_and_global_ref()
	_G.luact = require 'luact.init'
	_G.luact.initialize()
end


-- module function 
function _M.start(opts, executable)
	opts.init_proc = _G.luact and init_worker or init_worker_and_global_ref
	pulpo.initialize(opts)
	-- initialize deferred modules in luact
	pulpo_package.init_modules(exlib.LUACT_BUFFER, exlib.LUACT_IO)
	-- TODO : need to change pulpo configuration from commandline?
	_M.initialize()
	pulpo.run(opts, executable)
end
function _M.stop()
	pulpo.stop()
end
function _M.initialize(opts)
	opts = opts or {}
	local cmdl_args = optparse(_G.arg, opts_defs)
	util.merge_table(opts, cmdl_args)
	conn.initialize(opts)
	dht.initialize(opts)
	actor.initialize(opts)
	router.initialize(opts)

	local port = tonumber(opts.port or 8008)
	local proto = opts.proto or "tcp"
	if opts.serde then
		proto = proto .. "+" .. opts.serde
	end
	listener.unprotected_listen(proto.."://0.0.0.0:"..tostring(port))
	-- external port should be declared at each startup routine.
	-- because it is likely to open multiple listener port.
end
function _M.load(file)
	return actor.new(from_file, file, opts)
end
function _M.require(module)
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
return setmetatable(_M, {
	__call = function (t, target, ...)
		return actor.new(assert(factory[type(target)]), target, ...)
	end
})
