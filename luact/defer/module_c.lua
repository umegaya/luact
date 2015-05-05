local luact = require 'luact.init'
local original_loadfile = _G.loadfile
local original_require = _G.require
local fs = require 'pulpo.fs'
local _M = (require 'pulpo.package').module('luact.defer.module_c')
local process = luact.process

local deps = {} -- [root submodule path] = { [relative path] = { array of dependency path } }
local loaded = {} -- [commit hash] = { [module name] = { module table } }
local submodule_cache = {} -- [ path ] = "output of gitsubmodule"
local prefixes = { ".lua", fs.PATH_SEPS.."init.lua" }
local git_submodule_regex = '[%+%-%s]+(%w+)%s+([^%s]+)%s+%(.-%)'
_M.loaded = loaded
_M.deps = deps
-- indicate src depends on dst.
local function inject_dependency(src, dst)
--print('inject_dependency', src, dst)
	if not deps[dst] then
		deps[dst] = {}
	end
	table.insert(deps[dst], src)
end
local function traverse_dependency(changed, list)
	if list[changed] then
		return
	end
	list[changed] = true
	if not deps[changed] then
		return
	end
	for i=1,#deps[changed] do
		local f = deps[changed][i]
		if not list[f] then
			traverse_dependency(f, list)
			list[f] = true
		end
	end
end
function _M.add_dependency_entry(file)
	if not deps[file] then
		deps[file] = {}
	end
end
function _M.diff_recursive(prev, next, path, diffs, submods)
	---print('diff rec:', prev, next, path)
	path = path or "."
	diffs = diffs or {}
	submods = submods or {}
	-- print('cmdl', ("cd %s && git diff --name-only %s %s"):format(path, prev, next))
	local st, out = process.execute(("cd %s && git diff --name-only %s %s"):format(path, prev, next))
	if st ~= 0 then
		error('git diff error:'..tostring(st))
	end
	local st2, out2 = process.execute(("cd %s && git submodule"):format(path))
	if st2 ~= 0 then
		error('git submodule error:'..tostring(st2))
	end
	for hash, subpath in out2:gmatch(git_submodule_regex) do
		submods[path..fs.PATH_SEPS..subpath] = hash
	end
	for file in out:gmatch('[^%c]+') do
		file = path..fs.PATH_SEPS..file
		if submods[file] then
			_M.diff_recursive(prev, next, file, diffs, submods)
		else
			table.insert(diffs, file)
		end
	end
	return diffs, submods
end
function _M.compute_change_set(modified_files)
	local list = {}
	for i=1,#modified_files do
		local f = modified_files[i]
		traverse_dependency(f, list)
	end
	for i=1,#modified_files do
		local f = modified_files[i]	
		deps[f] = nil
	end
	return list
end
function _M.loadfile(file)
	local f, e = original_loadfile(file)
	if f then
		inject_dependency(debug.getinfo(2).source:sub(2), file)
	end
	return f, e
end
function _M.require(modname)
	local src = debug.getinfo(2).source:sub(2)
	local path = src:match('(.+)[/¥][^/¥]+$')
	local path_components = {}
	for name in path:gmatch('[^/¥]+') do
		table.insert(path_components, name)
	end
	-- will be module table if one is found.
	local mod
	--[[
		if require 'a.b.c' then relative module path expected to
		a/b/c.lua or a/b/c/init.lua. so convert a.b.c to a/b/c first
		then try both postfix ".lua" and "/init.lua".
	]]
	local base_module_path = modname:gsub("%.", fs.PATH_SEPS)
	local full_module_path
	--[[
		path == x/y/z/w.lua, then search .gitmodule with following order
		x/y/z/.gitmodule => dir = x/y/z
		x/y/.gitmodule => dir = x/y
		x/.gitmodule => dir = x
	]]
	for i=#path_components, 1, -1 do
		local dir = table.concat(path_components, fs.PATH_SEPS, 1, i) 
		if fs.exists(dir..fs.PATH_SEPS..".gitmodules") then
			-- in require, function cannot be yielded
			local ret = submodule_cache[dir]
			if not ret then
				ret = io.popen(('cd %s && git submodule'):format(dir)):read('*a')
				submodule_cache[dir] = ret
			end
			if #ret > 0 then
				-- if .gitmodule found, then parse git submodule output.
				-- its like: hash path (refspec)\n
				for hash, subpath in ret:gmatch(git_submodule_regex) do
					if base_module_path:match("^"..subpath) then
						for i=1,#prefixes do
							full_module_path = dir..fs.PATH_SEPS..base_module_path..prefixes[i]
							if fs.exists(full_module_path) then
							-- print('try load', full_module_path, hash, subpath)
								local m = loaded[hash]
								if m then
									mod = m[modname]
									if mod then return mod end
								else
									m = {}
									loaded[hash] = m
								end
								local f, e = original_loadfile(full_module_path)
								if not f then
									error(e)
								end
								mod = f(modname)
								m[modname] = mod
								goto inject_dependency
							end
						end
					end
				end
			end
		end
	end
::inject_dependency::
	if mod then
		inject_dependency(src, full_module_path)
	end
	return mod
end
function _M.invalidate_submodule_cache()
	for k,v in pairs(submodule_cache) do
		submodule_cache[k] = false
	end
end
-- remove loaded module table from _M.loaded which is no more used in app
function _M.gc(path)
	-- logger.info('>>> module gc start')
	-- 1. compute all commit hash which currently used in this repo (be careful for cyclic dependency)
	local t = {}
	fs.scan(path or ".", function (file)
		return fs.is_file(file) or file:match(fs.PATH_SEPS..'.git$')
	end, function (path, hs)
		--print('exists?', path..fs.PATH_SEPS..".gitmodules")
		if fs.exists(path..fs.PATH_SEPS..".gitmodules") then
			local st, ret = process.execute(('cd %s && git submodule'):format(path))
			if st == 0 and #ret > 0 then
				-- if .gitmodule found, then parse git submodule output.
				-- its like: (hash) (path) (refspec)\n
				for hash, subpath in ret:gmatch(git_submodule_regex) do
					hs[hash] = true
				end
			end
		end
	end, t)
	-- 2. remove value which correspond to key-hash which is no more used in this repo (based on 1.)
	for h1, _ in pairs(loaded) do
		local found
		for h2, _ in pairs(t) do
			if h1 == h2 then
				found = true
				break
			end
		end
		if not found then
			loaded[h1] = nil
		end
	end
	-- logger.info('>>> module gc end')
end

function _M.initialize()
end

return _M
