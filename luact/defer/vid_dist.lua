local ffi = require 'ffiex.init'
local uuid = require 'luact.uuid'
local memory = require 'pulpo.memory'
local exception = require 'pulpo.exception'
local dht = require 'luact.cluster.dht'

function _M.initialize(parent_address, dht_opts, vid_opts)
	dht.initialize(parent_address, dht_opts)
end
