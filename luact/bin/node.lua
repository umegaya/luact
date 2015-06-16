local node = require 'luact.iaas.cmd'
local uuid = require 'luact.uuid'
local serde = require 'luact.serde'
local json = serde[serde.kind.json]
local cmd = arg[1]
if cmd == "create" then
	local name, kind = arg[2], arg[3]
	local config = json:unpack_from_string(arg[4])
	local target_conf
	if #kind == 0 then
		for k,v in pairs(config) do
			target_conf = v
			break
		end
		assert(target_conf, "no entry exists in factory file:"..arg[4])
	else
		target_conf = assert(config[kind], "not supported provider:"..kind)
	end
	node.create({
		stdout = true,
	}, name, kind, target_conf)
elseif cmd == "rm" then
	local name = arg[2]
	node.rm({
		stdout = true,
	}, name)
elseif cmd == "ls" then
	node.ls({
		stdout = true,
	})
else
	assert(false, "no such command:"..cmd)
end

