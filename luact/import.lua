local ffi = require 'ffiex'
local memory = require 'luact.memory'
local _M = {}
local C = ffi.C
local _cache,_master,_mutex

ffi.cdef [[
	typedef struct c_parsed_cache {
		char *name, *cdecl, *macro;
	} c_parsed_cache_t;
	typedef struct c_parsed_info {
		int size, used;
		c_parsed_cache_t *list;
	} c_parsed_info_t;
]]

ffi.metatype('c_parsed_info_t', {
	__index = {
		init = function (t)
			t.size = 2
			t.used = 0
			t.list = memory.alloc_typed('c_parsed_cache_t', t.size)
		end,
		fin = function (t)
			if t.list ~= ffi.NULL then
				for i=0,(t.used - 1),1 do
					local e = t.list[i]
					memory.free(e.name)
					memory.free(e.cdecl)
					memory.free(e.macro)
				end
			end
		end,
		find = function (t, name)
			for i=0,(t.used - 1),1 do
				if ffi.string(t.list[i].name) == name then
					return t.list[i]
				end
			end
			return nil
		end,
		add = function (t, name, cdecl, macro)
			--> initialize parsed information
			if t.size <= t.used then
				t.list = memory.realloc_typed("c_parsed_cache_t", t.size * 2)
				t.size = (t.size * 2)
			end
			local e = t.list[t.used]
			e.cdecl = memory.strdup(cdecl)
			e.macro = memory.strdup(macro)
			e.name = memory.strdup(name)
			t.used = t.used + 1
			return e
		end,
	}
})

function _M.initialize(cache)
	if not cache then
		_master = true
		_cache = memory.alloc_typed('c_parsed_info_t')
		_cache:init()
	else
		_master = false
		_cache = cache
	end
end

function _M.get_cache_ptr()
	assert(_cache, "cache not initialized")
	return _cache
end

function _M.finalize()
	if _master then
		_cache:fin()
		memory.free(_cache)
	end
end

function inject_macros(state, symbols)
	local macro_decl = ""
	for _,sym in pairs(symbols) do
		local src = state.defs[sym]
		if type(src) == "number" then
			macro_decl = ("#define "..sym.." (" .. src .. ")")
		elseif type(src) == "string" then
			macro_decl = ("#define "..sym..' ("' .. src .. '")')
		else
			assert(false, "function type macro not supported:"..state.lcpp_macro_sources[sym])
		end
	end
	return macro_decl
end

function _M.import(name, cdecls, macros, paths, from)
	local c = _cache:find(name)
	if not c then
		local _macro, _cdecl
		local state = ffi.newstate()
		--> TODO : support platform which does not have gcc
		state:copt{ cc = "gcc" }
		if paths then
			for _,path in ipairs(paths) do
				state:path(path)
			end
		end
		state:parse(from)
		local parser = require 'ffiex.parser'
		_cdecl = parser.inject(state.tree, cdecls)
		--> initialize macro definition
		_macro = inject_macros(state, macros or {})
		--> initialize parsed information
		c = _cache:add(name, _cdecl, _macro)
	end
	ffi.cdef(ffi.string(c.macro))
	ffi.lcpp_cdef_backup(ffi.string(c.cdecl))
end

return _M
