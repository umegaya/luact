-- thin wrapper of docker-machine
local pulpo = require 'pulpo.init'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
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
			cb(io, buffer, ok)
		else
			ret = ret .. ffi.string(buffer, ok)
		end
	end
end
_M.wait = wait

local function exec(cmd, opts)
	opts = opts or {}
	if opts.stdout then
		return print(cmd)
	elseif opts.open_only then
		return process.open(cmd)
	else
		return process.execute(cmd)
	end
end

function _M.inspect(exec_opts, name)
	return exec(("docker-machine inspect %s"):format(name), exec_opts)
end

function _M.create(exec_opts, name, kind, opts)
	if not (opts and _M.create_opts) then
		exception.raise('invalid', 'not create options')
	end
	if not name then
		_M.machine_serial = _M.machine_serial + 1
		name = tostring(_M.machine_serial)
	end
	local k = kind or _M.kind
	local cmd = ("docker-machine create --driver=%s %s %s"):format(kind, build_create_opts(k, opts or _M.create_opts[k]), name)
	return exec(cmd, exec_opts), name
end

function _M.rm(exec_opts, name)
	return exec(("docker-machine rm %s"):format(name), exec_opts)
end

function _M.ls(exec_opts)
	return exec("docker-machine ls", exec_opts)
end
_M.exec = exec

function _M.initialize(opts)
	_M.kind = opts.kind
	if opts.create_opts then
		_M.create_opts = opts.create_opts
	end
	_M.machine_serial = 0
end

return _M
