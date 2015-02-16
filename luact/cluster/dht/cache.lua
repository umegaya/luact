local ffi = require 'ffiex.init'
local key = require 'luact.cluster.dht.key'

local _M = {}


-- cache
-- TODO : implement red black tree with FFI for faster insert/lookup
local cache_mt = {}
cache_mt.__index = cache_mt
function cache_mt:find(k, kl)
	kl = kl or #k
	for i=1,#self do
		-- logger.info(self[i].start_key, ("%02x"):format(k:byte()), 
		-- 	self[i].end_key:less_than_equals(k, kl), self[i].start_key:less_than_equals(k, kl))
		if not self[i].end_key:less_than_equals(k, kl) then
			if self[i].start_key:less_than_equals(k, kl) then
				return self[i]
			end
		end
	end
	return nil
end
function cache_mt:sort()
	table.sort(self, function (a, b)
		return a.start_key < b.start_key
	end)
end
function cache_mt:add(r)
	table.insert(self, r)
	self:sort()
end
function cache_mt:batch_add(ranges)
	for i=1,#ranges do
		table.insert(self, ranges[i])
	end
	self:sort()
end


-- module function 
function _M.new(kind)
	return setmetatable({kind = kind}, cache_mt)
end

return _M
