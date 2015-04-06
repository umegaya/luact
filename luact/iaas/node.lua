-- thin wrapper of docker-machine
local pulpo = require 'pulpo.init'
local memory = require 'pulpo.memory'
local process = pulpo.evloop.io.process
local _M = {}

local function build_create_opts(kind, opts)
	local cmd = ""
	opts = opts or _M.create_opts
	for k,v in pairs(opts) do
		cmd = cmd .. (" --%s-%s=%s"):format(kind, k, v)
	end
	return cmd
end

local buffer = memory.alloc_typed('char', 256)
local function wait(io, cb)
	local ret = ""
	while true do
		local ok, code = io:read(buffer, 256)
		if not ok then
			return code == 0 and ret
		elseif cb then
			cb(ptr, ok)
		else
			ret = ret .. ffi.string(buffer, ok)
		end
	end
end

local function exec(cmd, opts)
	if opts.stdout then
		return print(cmd)
	elseif opts.open_only then
		return process.open(cmd)
	else
		return tentacle(wait, process.open(cmd), opts.callback)
	end
end

function _M.create(name, exec_opts, kind, opts)
	local cmd = ("docker-machine create --driver=%s %s %s"):format(kind, build_create_opts(kind, opts), name)
	return exec(cmd, exec_opts)
end

function _M.rm(name, exec_opts)
	return exec(("docker-machine rm %s"):format(name), exec_opts)
end

function _M.ls(exec_opts)
	return exec("docker-machine ls", exec_opts)
end
_M.exec = exec

function _M.initialize(kind, opts)
	_M.kind = kind
	_M.create_opts = opts
end

return _M
