local ffi = require 'ffiex.init'
local range = require 'luact.cluster.dht.range'
local key = require 'luact.cluster.dht.key'

local _M = {}


-- cache
-- TODO : implement red black tree with FFI
local cache_mt = {}
cache_mt.__index = cache_mt
function cache_mt:find(k)
	for i=1,#self do
		if not self[i].end_key:less_than_equal(k, #k) then
			if not self[i].start_key:less_than(k, #k)
				return self[i]
			end
		end
	end
	-- 
	return nil
end
function cache_mt:metakey(k)
	return k 
end
function cache_mt:add(r)
	table.insert(self, r)
	table.sort(self, function (a, b)
		return a < b
	end)
end


-- module function 
function _M.new(kind)
	return setmetatable({kind = kind}, cache_mt)
end

return _M
