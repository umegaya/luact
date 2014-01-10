local lcpp = require 'luact.lcpp'
local originalCompileFile = lcpp.compileFile
local searchPath = {"./", "/usr/local/include/", "/usr/include/"}

lcpp.compileFile = function (filename, predefines)
	for _,path in ipairs(searchPath) do
		local trypath = (path .. filename)
		local ok, r = pcall(io.open, trypath, 'r')
		if ok and r then
			r:close()
			filename = trypath
			break
		end
	end
	-- print('file found:' .. filename)
	return originalCompileFile(filename, predefines)
end
local ffi = require 'ffi'
ffi.path = function (path)
	if path[#path] ~= '/' then
		path = (path .. '/')
	end
	table.insert(searchPath, path)
end
ffi.search = function (path, file, add)
	local p = io.popen(('find %s -name %s'):format(path, file), 'r')
	if not p then return nil end
	local line
	while true do
		line = p:read('*l')
		if not line then
			break -- eof
		else
			-- if matches find:, log of find itself. 
			if (not line:match('^find:')) and line:match((file .. '$')) then
				break
			end
		end
	end
	if line and add then
		--print('find path and add to header path:' .. line .. "|" .. line:gsub('^(.*/)[^/]+$', '%1'))
		ffi.path(line:gsub('^(.*/)[^/]+$', '%1'))
	end
	return line
end
ffi.define = function (defs)
	for k,v in pairs(defs) do
		ffi.lcpp_defs[k] = v
	end
end
ffi.undef = function (defs)
	for i,def in ipairs(defs) do
		ffi.lcpp_defs[def] = nil
	end
end
ffi.defs = setmetatable({}, {
	__index = function (t, k)
		local def = ffi.lcpp_defs[k]
		if type(def) == 'string' then
			local ok, r = pcall(loadstring, "return " .. def)
			if ok and r then 
				t[k] = r()
				return  t[k]
			end
		end
		t[k] = def
		return def
	end
})
ffi.csrc = function (src)
	local ppsrc = lcpp.compile()
end

-- basic compiler definition
-- TODO : get predefine from gcc's output?
ffi.define { __GNUC__ = 4 }
if ffi.arch == "x64" then
	ffi.define { __x86_64__ = true }
else
	ffi.define { __i386__ = true }
end
if ffi.os == 'OSX' then
--	ffi.define { __OSX_AVAILABLE_STARTING = "" }
end
return ffi
