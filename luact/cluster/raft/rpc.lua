local _M = {}

-- rpc for communicate between raft instance
_M.APPEND_ENTRIES = ffi.new('uint8_t', 1)
_M.REQUEST_VOTE = ffi.new('uint8_t', 2)
_M.INSTALL_SNAPSHOT = ffi.new('uint8_t', 3)
-- rpc for internal use
_M.INTERNAL_ACCEPTED = ffi.new('uint8_t', 10)
_M.INTERNAL_PROPOSE = ffi.new('uint8_t', 11)
_M.INTERNAL_ADD_REPLICA_SET = ffi.new('uint8_t', 12)
_M.INTERNAL_REMOVE_REPLICA_SET = ffi.new('uint8_t', 13)

return _M
