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
	opts.http = proto:match('^http') ~= nil
	local ln = p.listen(address, opts.proto_opts)
	tentacle(function (s, options)
		while true do
			local c = s:read()
			if not c then break end
			conn.from_io(c, options)
		end
	end, ln, opts)
	logger.debug('listen:'..proto..'+'..serde..'://'..address..' ('..(opts.internal and "internal" or "external")..")")
	return ln
end

local function check_internal_violation(proto, addr)
	local port = addr:match('([0-9]+)$')
	if _M.int_port and _M.int_port == port then
		raise('invalid', proto.."://"..addr, 'port already in use for internal service', _M.int_url)
	end
end

function _M.listen(url, opts)
	local proto, serde, address = conn.parse_hostname(url)
	check_internal_violation(proto, address)
	local ln = lmap[url]
	if not ln then
		assert((not opts) or (not opts.internal), exception.new('invalid', 'argument', 
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
	_M.int_port = address:match('([0-9]+)$')
	_M.int_url = url
end

return _M
