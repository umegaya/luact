local ffi = require 'ffiex.init'
local uuid = require 'luact.uuid'
local memory = require 'pulpo.memory'
local thread = require 'pulpo.thread'
local exception = require 'pulpo.exception'
local gen = require 'pulpo.generics'

local _M = (require 'pulpo.package').module('luact.defer.vid_c')

ffi.cdef(([[
typedef struct luact_vid_entry {
	uint32_t n_id:16, refc:%d, multi:1, dead:1, reserved:%d;
	union {
		struct luact_vid_group {
			luact_uuid_t *ids;
			uint16_t max, idx;
		} group;
		luact_uuid_t id;
	};
} luact_vid_entry_t;
]]):format(uuid.THREAD_BIT_SIZE, 14 - uuid.THREAD_BIT_SIZE))
ffi.cdef(([[
typedef struct luact_vid_manager {
  	%s map;
} luact_vid_manager_t;
]]):format(gen.mutex_ptr(gen.erastic_map('luact_vid_entry_t'))))


-- luact_vid_entry_t
local vid_ent_mt = {}
vid_ent_mt.__index = vid_ent_mt
function vid_ent_mt:init(ctor, ...)
	self.n_id = 1
	self.multi = 0
	self.dead = 0
	self.refc = 0
	self.id = ctor(...)
end
local copy_tmp = ffi.new('luact_uuid_t[1]')
function vid_ent_mt:add(ctor, ...)
	if self.multi ~= 0 then
		self:reserve(1)
	else
		copy_tmp[0] = self.id
		self:reserve(4)
		self.group.ids[0] = copy_tmp[0]
		assert(self.n_id <= 1)
	end
	local a = ctor(...)
	self.group.ids[self.n_id] = a
	self.n_id = self.n_id + 1
	return a
end
function vid_ent_mt:reserve(sz)
	if self.multi == 0 then
		self.group.ids = memory.alloc_typed('luact_uuid_t', sz)
		self.group.max = sz
		self.group.idx = 0
		self.multi = 1
	else
		local newsize = self.group.max
		while (self.n_id + sz) > newsize do
			newsize = newsize * 2
		end
		local tmp = memory.realloc_typed('luact_uuid_t', self.group.ids, newsize)
		if tmp ~= ffi.NULL then
			self.group.max = newsize
			self.group.ids = tmp
		else
			exception.raise('fatal', 'melloc fails')
		end
	end
end
function vid_ent_mt:choose()
	if self.multi ~= 0 then
		-- TODO : other strategy than round robin is needed?
		local tmp_idx = self.group.idx
		local tmp = self.group.ids[tmp_idx]
		self.group.idx = ((self.group.idx + 1) % self.n_id)
		return tmp, tmp_idx
	else
		return self.id
	end
end
function vid_ent_mt:alive()
	return self.dead == 0
end
function vid_ent_mt:refer()
	if self.refc >= (bit.lshift(1, uuid.THREAD_BIT_SIZE) - 1) then
		exception.raise('fatal', 'too many thread refer this vid')
	end
	self.refc = self.refc + 1
end
function vid_ent_mt:unref()
	self.refc = self.refc - 1
	return self.refc <= 0
end
function vid_ent_mt:remove(key, id, dtor)
	logger.report('vident:remove', self, self.id)
	if self.multi ~= 0 then
		local rmidx
		for i=0,self.n_id-1 do
			if not rmidx then
				if uuid.equals(self.group.ids[i], id) then
					rmidx = i 
					break
				end
			end
		end
		if rmidx then 
			if (rmidx < (self.n_id - 1)) then
				memory.move(self.group.ids + rmidx, self.group.ids + rmidx + 1, 
					(self.n_id - rmidx - 1) * ffi.sizeof('luact_uuid_t'))
			end
			dtor(key, id)
		end
	end
	self.n_id = self.n_id - 1
	if self.n_id <= 0 then
		logger.report('vid dead', key)
		self.dead = 1
	end
end
function vid_ent_mt:remove_all(key, dtor)
	if self.multi ~= 0 then
		for i=0,self.n_id-1 do
			dtor(key, self.group.ids[i])
		end
	else
		dtor(key, self.id)
	end
	logger.report('vid dead', key)
	self.dead = 1
end
function vid_ent_mt:fin()
	if self.multi ~= 0 then
		if self.group.ids ~= ffi.NULL then
			memory.free(self.group.ids)
			self.group.ids = ffi.NULL
		end
	end
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
function vid_manager_mt:encache(k, ent)
	ent:refer()
	rawset(self:cache(), k, ent)
end
function vid_manager_mt:decache(k)
	local ent = rawget(self:cache(), k)
	if ent then
		rawset(self:cache(), k, nil)
		if ent:unref() then
			self.map:touch(function (data, key) 
				logger.report('entry remove', key)
				data:remove(key)
			end, k)
		end
	end
end
-- should be called from mutex'ed code block
function vid_manager_mt:refresh_cache(map)
	local c = self:cache()
	for k,_ in pairs(c) do
		rawset(c, k, map:get(k))
	end
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
		elseif not ent:alive() then
			return nil
		end
		self:encache(k, ent)
	elseif not ent:alive() then
		self:decache(k)
		return nil
	end
	return ent:choose()
end
function vid_manager_mt:getent(k)
	return self.map:touch(function (data, key) 
		return data:get(key)
	end, k)
end
function vid_manager_mt:put(k, allow_multi, fn, ...)
	return self.map:touch(function (data, key, multi, ctor, ...) 
		local prev_size = data.size
		local ent,exists = data:put(key, function (ent, f, ...)
			ent.data:init(f, ...)
		end, ctor, ...)
		if prev_size ~= data.size then
			logger.notice('data allocation changed: refresh cache:', prev_size, data.size)
			self:refresh_cache(data)
		end
		if exists then
			if not ent:alive() then
				local refc = ent.refc
				ent:init(ctor, ...)
				ent.refc = refc
			elseif multi then
				return ent:add(ctor, ...)
			else
				exception.raise('invalid', 'already actor registered', k, ent.id)
			end
		else
			self:encache(key, ent)
		end
		if multi then
			local tmp = memory.managed_alloc_typed('luact_uuid_t')
			ffi.copy(tmp, ent.id, ffi.sizeof('luact_uuid_t'))
			return tmp
		else
			return ent.id
		end
	end, k, allow_multi, fn, ...)
end
function vid_manager_mt:remove(k, id, fn)
	local c = self:cache()
	rawset(c, k, nil)
	self.map:touch(function (data, key, dtor, uid) 
		local ent = data:get(key)
		if ent then
			if uid then
				if ent:remove(key, uid, dtor) then 
					self:decache(k, ent)
				end
			else
				ent:remove_all(key, dtor)
				self:decache(k, ent)
			end
		end
	end, k, fn, id)
end
function vid_manager_mt:refresh(k)
	self:decache(k)
end
ffi.metatype('luact_vid_manager_t', vid_manager_mt)


-- module function 
function _M.initialize(opts)
	_M.dht = thread.shared_memory("luact_vidmap", function (sz)
		local p = memory.alloc_typed('luact_vid_manager_t')
		p:init(sz)
		return 'luact_vid_manager_t', p
	end, opts.local_map_initial_size or 4096)
end

function _M.unregister_actor(vid, id)
	_M.dht:remove(vid, id, function (key, a)
		local tid = uuid.thread_id(a)
		if tid == _M.thread_id then
			actor.destroy(a)
		end
	end, vid)
end

return _M
