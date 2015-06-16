local luact = require 'luact.init'
local cmd = require 'luact.iaas.cmd'
local actor = require 'luact.actor'
local uuid = require 'luact.uuid'
local json = require 'luact.serde.json'
local exception = require 'pulpo.exception'
local socket = require 'pulpo.socket'
local pulpo = require 'pulpo.init'
local process = pulpo.evloop.io.process
local tentacle = require 'pulpo.tentacle'
local _M = {}

local function dyn_config_cmdl(ip, opts)
	local cmdl = {}
	table.insert(cmdl, "startup_at="..tostring(uuid.epoc()))
	table.insert(cmdl, "parent_address="..tostring(uuid.node_address))
	table.insert(cmdl, "local_address="..tostring(ip))
	table.insert(cmdl, "node.kind="..cmd.kind)
	table.insert(cmdl, "node.create_opts='"..json:pack_to_string(cmd.create_opts).."'")
	if opts then
		for k,v in pairs(opts) do
			table.insert(cmdl, k.."="..v)
		end
	end
	return "-l "..table.concat(cmdl, "-l ")
end

function _M.new(image, opts)
	local code, name, ip, out
	-- create node for new container
	code, name = cmd.create()
	if code == 0 then
		exception.raise('syscall', 'node create', code)
	end
	logger.info('create new node for container', name)
	-- retrieve new node's ip. it will be
	code, out = cmd.inspect(nil, name)
	if code == 0 then
		exception.raise('syscall', 'get node ip', code)
	end	
	ip = json:unpack_from_string(out)["PrivateIPAddress"]
	-- launch yue container
	code = process.execute(('MACHINE_CMD=docker-machine yue --daemon --restart -H %s -w / %s %s'):format(name, dyn_config_cmdl(ip, opts), image))
	if code == 0 then
		exception.raise('syscall', 'launch container', code, out)
	end
	local machine_id = socket.numeric_ipv4_addr_by_host(ip)
	logger.info('launch new container at', ip, ('%x'):format(machine_id))
	return actor.root_of(machine_id, 1)
end

function _M.initialize(opts)
	cmd.initialize(opts)
end

return _M
