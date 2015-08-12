local _M = {}

-- data category
-- system kind
_M.KIND_META1 = 1
_M.KIND_META2 = 2
_M.KIND_VID = 3
_M.KIND_STATE = 4
-- definitions of boundary values
_M.ROOT_METADATA_KIND = _M.KIND_META1
_M.NON_METADATA_KIND_START = _M.KIND_META2 + 1
_M.NON_METADATA_KIND_END = _M.KIND_STATE
_M.BUILTIN_KIND_START = 0x01
_M.BUILTIN_KIND_END = 0x3f
-- user kind
_M.USER_KIND_PREFIX_START = 0x40
_M.USER_KIND_PREFIX_END = 0xfe
_M.NUM_USER_KIND = _M.USER_KIND_PREFIX_END - _M.USER_KIND_PREFIX_START

-- key prefix definitions
-- syskey prefix
-- syskey is key which is non-metakey but must exist in root range. 
_M.SYSKEY_CATEGORY_KIND = 0
-- collocation key prefix
-- collocation key is a key force to move with specified key. that is, key and collocation key must always be in the same range
_M.COLOC_TXN = 1

return _M
