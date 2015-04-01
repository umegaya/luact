-- (should be) thin wrapper of docker-machine
local pulpo = require 'pulpo.init'
local memory = require 'pulpo.memory'
local process = pulpo.evloop.io.process
local _M = {}

local function build_opts(opts)
	local cmd = ""
	if opts then
		for k,v in pairs(opts) do
			for kk,vv in pairs(v) do
				cmd = cmd .. ("--%s-%s=%s"):format(k, kk, vv)
			end
		end
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

local function exec(cmd, name, opts)
	local cmdl = ("docker-machine %s %s %s"):format(cmd, build_opts(opts), name)
	if opts.open_only then
		return process.open(cmdl)
	else
		return tentacle(wait, process.open(cmdl), opts.callback)
	end
end

function _M.create(name, opts)
	return exec("create", name, opts)
end

function _M.destroy(name, opts)
	return exec("rm", name, opts)
end

function _M.list()
	return exec("ls", "")
end

_M.exec = exec

return _M
