local ffi = require 'ffiex.init'
local uuid = require 'luact.uuid'
local memory = require 'pulpo.memory'
local gen = require 'pulpo.generics'

local _M = (require 'pulpo.package').module('luact.defer.vid_c')

ffi.cdef [[
typedef struct luact_vid_entry {
	luact_uuid_t id;
} luact_vid_entry_t;
]]
ffi.cdef(([[
typedef struct luact_vid_manager {
  	%s map;
} luact_vid_manager_t;
]]):format(gen.mutex_ptr(gen.erastic_map('luact_vid_entry_t'))))


-- luact_vid_entry_t
local vid_ent_mt = {}
vid_ent_mt.__index = vid_ent_mt
function vid_ent_mt:init(ctor, ...)
	self.id = ctor(...)
end
ffi.metatype('luact_vid_entry_t', vid_ent_mt)


-- luact_vid_manager_t
local vid_manager_mt = {}
vid_manager_mt.__cache = {}
vid_manager_mt.__index = vid_manager_mt
function vid_manager_mt:init(size)
	self.map:init(function (data, sz) return data:init(sz) end, size)
end
function vid_manager_mt:cache()
	return vid_manager_mt.__cache
end
function vid_manager_mt:get(k)
	local c = self:cache()
	local ent = rawget(c, k)
	if not ent then
		ent = self.map:touch(function (data, key) 
			return data:get(key)
		end, k)
		if not ent then
			return nil
		end
		rawset(c, k, ent)
	end
	return ent.id
end
function vid_manager_mt:put(k, fn, ...)
	return self.map:touch(function (data, key, ctor, ...) 
		return data:put(key, function (ent, f, ...)
			ent.data:init(f, ...)
		end, ctor, ...)
	end, k, fn, ...)
end
function vid_manager_mt:remove(k, fn)
	local c = self:cache()
	rawset(c, k, nil)
	self.map:touch(function (data, dtor) 
		local ent = data:get(k)
		if ent then dtor(ent.id) end
		data:remove(k)
	end, fn)
end
function vid_manager_mt:refresh(k)
	local c = self:cache()
	rawset(c, k, nil)
	return self:get(k)	
end
ffi.metatype('luact_vid_manager_t', vid_manager_mt)


-- module function 
local vid_mt
function _M.initialize(mt, size)
	vid_mt = mt
	_M.dht = memory.alloc_typed('luact_vid_manager_t')
	_M.dht:init(size or 4096)
end

function _M.new(url)
	local host, path = url:match('([^%+]-%+?[^%+]*://[^/]+)(.*)')
	if not path then 
		return nil 
	end
	return setmetatable({host=host,path=path}, vid_mt)
end

return _M
