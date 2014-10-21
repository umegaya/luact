local conn = require 'luact.conn'
local actor = require 'luact.actor'
local exception = require 'pulpo.exception'
local tentacle = require 'pulpo.tentacle'
local raise = exception.raise

local _M = {}
local lmap = {}

function new_listener(url, opts)
	local proto, serde, address = url:find('([^%+]+)%+?([^%+]*)://(.+)')
	if not proto then raise('invalid', 'url', url) end
	if #serde <= 0 then serde = conn.DEFAULT_SERDE end
	opts.serde = serde
	local p = pulpo.evloop.io[proto]
	assert(p.listen, exception.new('not_found', 'method', 'listen', proto))
	local ln = p.listen(address, opts)
	tentacle(function (s, options)
		while true do
			conn.from_io(s:read(), options)
		end
	end, ln, opts)
	logger.info('listen:'..url..'('..(opts.internal and "int" or "ext")..")")
	return ln
end

function _M.listen(url, opts)
	local ln = lmap[url]
	if not ln then
		assert(opts.internal, exception.new('invalid', 'argument', 
			"please use listener.reliable_listen to open internal listner"))
		ln = actor.new(new_listener, url, opts)
		lmap[url] = ln
	end
	return ln
end

function _M.unprotected_listen(url, opts)
	local ln = lmap[url]
	if not ln then
		opts.internal = true
		ln = actor.new(new_listener, url, opts)
		lmap[url] = ln
	end
	return ln
end

return _M
