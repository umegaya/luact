local conn = require 'luact.conn'
local actor = require 'luact.actor'
local pulpo = require 'pulpo.init'
local exception = require 'pulpo.exception'
local tentacle = require 'pulpo.tentacle'
local raise = exception.raise

local _M = {}
local lmap = {}

function new_listener(proto, serde, address, opts)
	local p = pulpo.evloop.io[proto]
	assert(p.listen, exception.new('not_found', 'method', 'listen', proto))
	opts.serde = serde
	local ln = p.listen(address, opts.proto_opts)
	tentacle(function (s, options)
		while true do
			local c = s:read()
			logger.info(address, 'accept')
			conn.from_io(c, options)
		end
	end, ln, opts)
	logger.info('listen:'..proto..'+'..serde..'://'..address..' ('..(opts.internal and "internal" or "external")..")")
	return ln
end

function _M.listen(url, opts)
	local proto, serde, address = conn.parse_hostname(url)
	local ln = lmap[address]
	if not ln then
		assert((not ops) or (not opts.internal), exception.new('invalid', 'argument', 
			"please use unprotected_listen to open internal listener"))
		ln = actor.new(new_listener, proto, serde, address, opts or {})
		lmap[address] = ln
	end
	return ln
end

function _M.unprotected_listen(url, opts)
	local proto, serde, address = conn.parse_hostname(url)
	local ln = lmap[address]
	if not ln then
		if opts then
			opts.internal = true
		else
			opts = { internal = true }
		end
		ln = actor.new(new_listener, proto, serde, address, opts)
		lmap[address] = ln
	end
	return ln
end

return _M
