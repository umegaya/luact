local exlib = require 'luact.exlib'

local pulpo = require 'pulpo.init'
local pulpo_package = require 'pulpo.package'
-- pulpo_package.DEBUG= true
local ffi = require 'ffiex.init'

local actor = require 'luact.actor'
local optparse = require 'luact.optparse'
local listener = require 'luact.listener'
local dht = require 'luact.dht'
local pbuf = require 'luact.pbuf'
local router = require 'luact.router'
local conn = require 'luact.conn'

local _M = {}

-- command line option definitions
local opts_defs = {
	{"a", "local_address"},
	{"t", "startup_at"},
	{"p", "port"},
	{nil, "proto"},
}

-- local function
local factory = {
	["table"] = function (tbl, opts)
		return actor.new(function (t)
			return t
		end, tbl)
	end,
	["cdata"] = function (cdata, opts)
		return actor.new(function (c)
			return c
		end, cdata)
	end,
	["function"] = function (fn, opts)
	end,
	["file"] = function (file, opts)
	end,
	["module"] = function (module, opts)
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
function _M.initialize()
	local cmdl_args = optparse(_G.arg, opts_defs)
	conn.initialize(cmdl_args)
	dht.initialize(cmdl_args)
	actor.initialize(cmdl_args)

	local port = tonumber(cmdl_args.port or 8008)
	local proto = cmdl_args.proto or "tcp"
	if cmdl_args.serde then
		proto = proto .. "+" .. cmdl_args.serde
	end
	listener.unprotected_listen(proto.."://0.0.0.0:"..tostring(port))
	-- external port should be declared at each startup routine.
	-- because it is likely to open multiple listener port.
end
function _M.load(file, opts)
	return from_file(file, opts)
end
function _M.require(module, opts)
	return from_module(file, opts)
end
return setmetatable(_M, {
	__call = function (t, target, opts, ...)
		return assert(factory[type(target)])(target, opts, ...)
	end
})
