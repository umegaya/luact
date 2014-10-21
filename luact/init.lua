require 'luact.exlib'
_G.pulpo = require 'pulpo.init'
_G.ffi = require 'ffiex.init'
local actor = require 'luact.actor'
local opts_parser = require 'luact.opts'
local listener = require 'luact.listener'
local dht = require 'luact.dht'
local pbuf = require 'luact.pbuf'
local router = require 'luact.router'
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
	["cdata"] = function (tbl, opts)
		return actor.new(function (t)
			return t
		end, tbl)
	end,
	["function"] = function (fn, opts)
	end,
	["file"] = function (file, opts)
	end,
	["module"] = function (module, opts)
	end,
}
local from_file, from_module = factory["file"], factory["module"]


-- module function 
local _M = {}
function _M.start(opts, executable)
	opts.init_proc = _M.initialize
	pulpo.initialize(opts)
	-- TODO : need to change pulpo configuration from commandline?
	_M.initialize()
	pulpo.run(opts, executable)
end
function _M.initialize()
	local cmdl_args = opts_parser(_G.arg, opts_defs)
	conn.initialize(cmdl_args)
	dht.initialize(cmdl_args)
	actor.initialize(cmdl_args)
	pbuf.initialize(cmdl_args)
	router.initialize(cmdl_args)

	local port = tonumber(cmdl_args.port or 8008)
	local proto = cmdl_args.proto or "tcp"
	listener.unprotected_listen(proto.."://0.0.0.0:"..tostring(port))
end
function _M.load(file, opts)
	return from_file(file, opts)
end
function _M.require(module, opts)
	return from_module(file, opts)
end
return setmetatable(_M, {
	__call = function (t, opts, ...)
		return assert(factory[type(t)])(t, opts, ...)
	end
})
