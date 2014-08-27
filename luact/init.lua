_G.pulpo = require 'lib.pulpo'
_G.ffi = require 'lib.ffiex'
local actor = require 'luact.actor'

-- local function
local factory = {}
function factory.table(ctor, tbl, opts)
	(opts.link and actor.new_link or actor_new)()
end
function factory.cdata(data, opts)
end
function factory.function(fn, opts)
end
function factory.file(file, opts)
end
function factory.module(module, opts)
end
local function parse_args(args)
	return args or {}
end
local function configure(actor, opts)
end


-- module function 
local _M = {}
function _M.start(opts, executable)
	opts.args = parse_args(opts.args)
	pulpo.initialize(opts)
	actor.initialize(opts)
	pulpo.run(opts, executable)
end

function _M.load(file, opts)
	return factory.file(file, opts)
end
function _M.require(module, opts)
	return factory.module(file, opts)
end
return setmetatable(_M, {
	__call = function (t, src, opts)
		return assert(factory[type(t)])(src)
	end
})
