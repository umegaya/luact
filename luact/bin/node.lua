local node = require 'luact.iaas.node'
local uuid = require 'luact.uuid'
local json = require 'dkjson'
local kind = arg[1]
local config = json.decode(arg[2])
local target_conf = assert(config[kind])

node.create(tostring(uuid.new()), target_conf)

return true
