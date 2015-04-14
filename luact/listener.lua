local conn = require 'luact.conn'
local actor = require 'luact.actor'
local pulpo = require 'pulpo.init'
local exception = require 'pulpo.exception'
local tentacle = require 'pulpo.tentacle'
local raise = exception.raise

local _M = {}
local lmap = {}

local function new_listener(proto, serde, address, opts)
	local p = pulpo.evloop.io[proto]
	assert(p.listen, exception.new('not_found', 'method', 'listen', proto))
	opts.serde = serde
	local ln = p.listen(address, opts.proto_opts)
	tentacle(function (s, options)
		while true do
			local c = s:read()
			logger.debug(address, 'accept')
			conn.from_io(c, options)
		end
	end, ln, opts)
	logger.debug('listen:'..proto..'+'..serde..'://'..address..' ('..(opts.internal and "internal" or "external")..")")
	return ln
end

local function check_internal_violation(proto, addr)
	if proto == _M.int_proto and addr == _M.int_addr then
		raise('invalid', 'addr already in use for internal service', proto.."://"..addr)
	end
end

function _M.listen(url, opts)
	local proto, serde, address = conn.parse_hostname(url)
	check_internal_violation(proto, address)
	local ln = lmap[url]
	if not ln then
		assert((not ops) or (not opts.internal), exception.new('invalid', 'argument', 
			"please use unprotected_listen to open internal listener"))
		ln = actor.new(new_listener, proto, serde, address, opts or {})
		lmap[url] = ln
	end
	return ln
end

function _M.unprotected_listen(url, opts)
	local proto, serde, address = conn.parse_hostname(url)
	check_internal_violation(proto, address)
	local ln = lmap[url]
	if not ln then
		if opts then
			opts.internal = true
		else
			opts = { internal = true }
		end
		ln = actor.new(new_listener, proto, serde, address, opts)
		lmap[url] = ln
	end
	return ln
end

function _M.set_intenral_url(url)
	local proto, serde, address = conn.parse_hostname(url)
	_M.int_proto = proto
	_M.int_addr = address
end

return _M
