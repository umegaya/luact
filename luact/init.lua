_G.pulpo = require 'lib.pulpo'
_G.ffi = require 'lib.ffiex'
local actor = require 'luact.actor'

-- local function
local factory = {}
function factory.string(str)
end
function factory.table(tbl)
end
function factory.cdata(data)
end


-- module function 
local _M = {}
function _M.start(opts, executable)
	pulpo.initialize(opts)
	actor.initialize(opts)
	pulpo.run(opts, executable)
end
return setmetatable(_M, {
	__call = function (t, src)
		return _M[type(t)](src)
	end
})
