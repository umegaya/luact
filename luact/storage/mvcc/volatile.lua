local mvcc = require 'luact.storage.mvcc'
local exception = require 'pulpo.exception'

local _M = {}

-- local functions
local function new_mvcc(db)
	local p = memory.alloc_typed('luact_mvcc_rocksdb_t')
	p:init(db)
	return p
end
local function new_mvcc_cf(cf)
	local p = memory.alloc_typed('luact_mvcc_rocksdb_cf_t')
	p:init(cf)
	return p
end


-- volatile db
local volatile_db_mt = {}
volatile_db_mt.__index = volatile_db_mt
function volatile_db_mt:rawget(k, kl, opts)
	local v = self[ffi.string(k, kl)]
	if v then
		return v, #v
	else
		return nil, 0
	end
end
function volatile_db_mt:rawput(k, kl, v, vl, opts)
	self[ffi.string(k, kl)] = ffi.string(v, vl)
end


-- mvcc volatile
local mvcc_volatile_mt = mvcc.new_mt()

-- module functions
function _M.open()
	return setmetatable(new_volatile_db, mvcc_volatile_mt)
end
function _M.close(mvcc)
end

return _M
