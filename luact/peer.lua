local ffi = require 'ffiex.init'
local memory = require 'pulpo.memory'
local actor = require 'luact.actor'
local uuid = require 'luact.uuid'
local conn = require 'luact.conn'
local _M = {}

-- luact peer
local methods_cache = {}
local cache = {}
local function peer_caller_proc(t, ...)
	return t.id.conn:dispatch(t, ...)
end
local peer_caller_mt = {
	__call = peer_caller_proc,
}
local peer_metatable = {
	__index = function (t, k)
		local v = rawget(methods_cache, k)
		-- cache not exist or in-use
		if v then
			if v.id then -- cache exist but in-use
				-- copy on write
				v = setmetatable(util.copy_table(v), peer_caller_mt)
				rawset(methods_cache, k, v)
			end
		else -- cache not exist
			local name, flag = actor.parse_method_name(k)
			v = setmetatable({method = name, flag = flag}, peer_caller_mt)
			rawset(methods_cache, k, v)
		end
		v.id = t
		return v
	end,
	alloc = function ()
		if #cache > 0 then
			return table.remove(cache)
		else
			return memory.alloc_typed('luact_peer_t')
		end
	end,
	__gc = function (p)
		table.insert(cache, p)
	end,
}


-- module functions
function _M.new(peer_id)
	return conn.new_peer(peer_id)
end
function _M.path(peer, path)
	return setmetatable({conn = peer, path = path}, peer_metatable)
end
function _M.close(peer)
	if type(peer) == 'table' then
		peer = peer.conn
	elseif type(peer) ~= 'cdata' then
		exception.raise('invalid', 'it should not be peer object', type(peer))
	end
	peer:close()
	memory.free(peer)
end

return _M
