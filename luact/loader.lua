local ffi = require 'ffiex'
local parser = require 'ffiex.parser'
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
		char *path;
		c_parsed_cache_t *list;
	} c_parsed_info_t;
]]

ffi.metatype('c_parsed_info_t', {
	__index = {
		init = function (t, path)
			t.size = 4
			t.used = 0
			t.list = assert(memory.alloc_typed('c_parsed_cache_t', t.size), 
				"fail to allocate loader cache:"..t.size)
			if path then
				t.path = memory.strdup(path)
			end
		end,
		fin = function (t)
			if t.list ~= ffi.NULL then
				for i=0,(t.used - 1),1 do
					local e = t.list[i]
					memory.free(e.name)
					memory.free(e.cdecl)
					memory.free(e.macro)
				end
				memory.free(t.list)
			end
			if t.path ~= ffi.NULL then
				memory.free(t.path)
			end
		end,
		find = function (t, name)
			for i=0,(t.used - 1),1 do
				if ffi.string(t.list[i].name) == name then
					return t.list[i]
				end
			end
			return t:load(name)
		end,
		reserve_entry = function (t)
			--> initialize parsed information
			if t.size <= t.used then
				local p = memory.realloc_typed("c_parsed_cache_t", t.list, t.size * 2)
				if p then
					t.list = p
					t.size = (t.size * 2)
				else
					print('loaderer:add:realloc fails:'..tostring(t.size * 2))
					return nil
				end
			end
			return t.list[t.used]
		end,
		commit_entry = function (t)
			t.used = t.used + 1
		end,
		add = function (t, name, cdecl, macro, from_load)
			local e = t:reserve_entry()
			e.cdecl = memory.strdup(cdecl)
			e.macro = memory.strdup(macro)
			e.name = memory.strdup(name)
			if not from_load then
				t:save(e)
			end
			t:commit_entry()
			return e
		end,
		save = function (t, e)
			local root = ffi.string(t.path)
			for _,kind in ipairs({"cdecl", "macro"}) do
				local path = root .. "/" .. ffi.string(e.name) .. "." .. kind
				local f = assert(io.open(path, "w"), "cannot open for write:"..path)
				-- print('save:'..ffi.string(e[kind]))
				f:write(ffi.string(e[kind]))
				f:close()
			end
		end,
		load = function (t, name)
			local root = ffi.string(t.path)
			local e = t:reserve_entry()
			e.name = memory.strdup(name)
			for _,kind in ipairs({"cdecl", "macro"}) do
				local path = root .. "/" .. name .. "." .. kind
				local f = io.open(path, "r")
				if not f then return nil end
				e[kind] = memory.strdup(f:read('*a'))
				f:close()
			end
			t:commit_entry()
			return e
		end,
	}
})

function _M.initialize(cache, loader_ffi_state)
	if loader_ffi_state then
		_M.ffi_state = loader_ffi_state
	else
		_M.ffi_state = ffi.newstate()
		_M.ffi_state:copt { cc = "gcc" }
		_M.ffi_state:path "/usr/local/include/luajit-2.0"
	end
	if cache then
		_master = false
		_cache = cache
		_M.set_cache_dir(ffi.string(_cache.path))
	else
		_master = true
		_cache = assert(memory.alloc_typed('c_parsed_info_t'), "fail to alloc cache")
		_cache:init(_M.cache_dir)
	end
	--> initialize lazy init module
	for _,mod in ipairs(_M.lazy_init_modules) do
		mod.init_cdef()
	end
end

_M.lazy_init_modules = {}
function _M.add_lazy_init(module)
	table.insert(_M.lazy_init_modules, module)
end

function _M.set_cache_dir(path)
	-- print('set cache dir:'..path)
	_M.cache_dir = path
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
			macro_decl = (macro_decl .. "#define "..sym.." (" .. src .. ")\n")
		elseif type(src) == "string" then
			macro_decl = (macro_decl .. "#define "..sym..' ("' .. src .. '")\n')
		else
			assert(false, sym..": not supported type:"..type(src))
		end
	end
	return macro_decl
end

function merge_nice_to_have(cdecls_or_macros)
	if type(cdecls_or_macros.nice_to_have) ~= 'table' then
		return cdecls_or_macros
	else
		local ret = {}
		for _,elem in ipairs(cdecls_or_macros) do
			table.insert(ret, elem)
		end
		for _,elem in ipairs(cdecls_or_macros.nice_to_have) do
			table.insert(ret, elem)
		end
		return ret
	end
end
function merge_regex(tree, macros)
	if type(macros.regex) ~= 'table' then
		return macros
	else
		local ret = {}
		for _,elem in ipairs(macros) do
			table.insert(ret, elem)
		end
		for _,pattern in ipairs(macros.regex) do
			for k,v in pairs(_M.ffi_state.lcpp_defs) do
				if k:find(pattern) and type(v) ~= 'function' then
					table.insert(ret, k)
				end
			end
		end
		return ret
	end
end

function _M.unsafe_load(name, cdecls, macros, lib, from)
	local c = _cache:find(name)
	tmp_cdecls = merge_nice_to_have(cdecls)
	tmp_macros = merge_nice_to_have(macros)
	if not c then
		_M.ffi_state:parse(from)
		local _cdecl = parser.inject(_M.ffi_state.tree, tmp_cdecls, ffi.imported_csymbols)
		--> initialize macro definition
		tmp_macros = merge_regex(_M.ffi_state.tree, tmp_macros)
		local _macro = inject_macros(_M.ffi_state, tmp_macros)
		--> initialize parsed information
		c = assert(_cache:add(name, _cdecl, _macro), "fail to cache:"..name)
	else
		--- print('macro:'..ffi.string(c.macro))
		_M.ffi_state:cdef(ffi.string(c.macro))
	end
	-- print('cdecl:'..ffi.string(c.cdecl))
	ffi.lcpp_cdef_backup(ffi.string(c.cdecl))
	local clib
	if lib then
		clib = assert(ffi.load(lib), "fail to load:" .. lib)
	else
		clib = ffi.C
	end
	for _,decl in ipairs(cdecls) do
		local s,e = decl:find('%s+')
		if s then
			decl = decl:sub(e+1)
		end
		local ok, r = pcall(getmetatable(clib).__index, clib, decl)
		if not ok then
			ok, r = pcall(ffi.sizeof, decl) 
			if not ok then
				for _,pfx in ipairs({"struct", "union", "enum"}) do
					ok = pcall(ffi.sizeof, pfx .. " " .. decl)
					if ok then 
						--print(decl .. " is " .. pfx)
						break 
					end
				end
			end
		else
			--print(decl .. " is func")
		end
		if not ok then
			assert(false, "declaration of "..decl.." not available")
		end
	end
	for _,macro in ipairs(macros) do
		assert(_M.ffi_state.defs[macro], "definition of "..macro.." not available")
	end
	return _M.ffi_state, clib
end

local msg = [[

###########################
loader fails by error:

%s

%s
###########################

]]
local caution = [[it may caused by outdated cdef cache. 
so luact will remove cache dir. 
try restart to recreate cdef cache.]]
function _M.load(name, cdecls, macros, lib, from)
	local i = 0
	local ret = {pcall(_M.unsafe_load, name, cdecls, macros, lib, from)}
	if not table.remove(ret, 1) then
		local err = table.remove(ret, 1)
		if _M.cache_dir then
			local util = require 'luact.util'
			print(msg:format(err .. "\n" .. debug.traceback(), caution))
			util.rmdir(_M.cache_dir)
		else
			print(msg:format(err .. "\n" .. debug.traceback(), ""))
		end
		os.exit(-1)
	end
	return unpack(ret)
end

return _M
