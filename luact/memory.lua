local ffi = require 'ffiex'
local _M = {}
local C = ffi.C

-- malloc information list
local malloc_info_list = setmetatable({}, {
	__index = function (t, k)
		local ct = ffi.typeof(k)
		local v = { t = k.."*", sz = ffi.sizeof(ct) }
		rawset(t, k, v)
		return v
	end
})

-- hmm, I wanna use importer(import.lua) but dependency for thread safety
-- prevents from refering pthread_*...
-- but I believe these interface never changed at 100 years future :D
-- TODO : using jemalloc
ffi.cdef [[
	void *malloc(size_t);
	void free(void *);
	void realloc(void *, size_t);
	char *strncpy(char *, const char *, size_t);
]]

function _M.alloc(sz)
	local p = ffi.gc(C.malloc(sz), nil)
	return p ~= ffi.NULL and p or nil
end
	
function _M.alloc_typed(ct, sz)
	local malloc_info = malloc_info_list[ct]
	local p = _M.alloc((sz or 1) * malloc_info.sz)
	if not p then return p end
	return ffi.cast(malloc_info.t, p)
end

function _M.strdup(str)
	local p = _M.alloc_typed('char', #str + 1)
	if p then
		C.strncpy(p, str, #str + 1)
	end
	return p
end

function _M.realloc(p, sz)
	local p = ffi.gc(C.realloc(p, sz), nil)
	return p ~= ffi.NULL and p or nil
end

function _M.realloc_typed(ct, p, sz)
	local malloc_info = malloc_info_list[ct]
	local p = _M.realloc(p, malloc_info.sz * (sz or 1))
	if not p then return p end	
	return ffi.cast(malloc_info.t, p)
end

function _M.managed_alloc(sz)
	local p = ffi.gc(C.malloc(sz), C.free)
	return p ~= ffi.NULL and p or nil
end

function _M.managed_alloc_typed(ct, sz)
	local malloc_info = malloc_info_list[ct]
	local p = _M.managed_alloc((sz or 1) * malloc_info.sz)
	if not p then return p end	
	return ffi.cast(malloc_info.t, p)
end

function _M.managed_realloc(p, sz)
	local p = ffi.gc(C.realloc(p, sz), C.free)
	return p ~= ffi.NULL and p or nil
end

function _M.managed_realloc_typed(ct, p, sz)
	local malloc_info = malloc_info_list[ct]
	local p = _M.realloc(p, malloc_info.sz * (sz or 1))
	if not p then return p end
	return ffi.cast(malloc_info.t, p)
end

function _M.free(p)
	C.free(p)
end

return _M
