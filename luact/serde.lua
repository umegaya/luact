local _M = {}
local kind = {
	serpent = 1,
	json = 2,
	msgpack = 3,
	protobuf = 4,
}
_M.kind = kind
-- serpent
_M[kind.serpent] = require 'luact.serde.serpent'
-- json
_M[kind.json] = require 'luact.serde.json'
-- msgpack
_M[kind.msgpack] = require 'luact.serde.msgpack'
-- protobuf (TODO)
_M[kind.protobuf] = require 'luact.serde.protobuf'

return _M
