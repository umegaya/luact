local _M = (require 'pulpo.package').module('luact.defer.pbuf_c')

-- deps
local ffi = require 'ffiex.init'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
exception.define('rocksdb')

-- cdefs 
local C = ffi.C
ffi.cdef[[
#include <rocksdb/c.h>
typedef struct luact_rocksdb_column_family {
	rocksdb_t *db;
	rocksdb_column_family_handle_t *cf;
} luact_rocksdb_column_family_t;
typedef struct luact_rocksdb_txn {
	rocksdb_t *db;
	rocksdb_writebatch_t *b;
} luact_rocksdb_txn_t;
typedef struct luact_rocksdb_cf_txn {
	rocksdb_t *db;
	rocksdb_column_family_handle_t *cf;	
	rocksdb_writebatch_t *b;
} luact_rocksdb_txn_cf_t;
]]

-- local functions
local errptr = ffi.new('char*[1]')
local function callapi(fn, ...)
	errptr[0] = util.NULL
	local r = fn(..., errptr)
	if errptr[0] ~= util.NULL then
		exception.raise('rocksdb', 'api', errptr[0])
	end
	return r
end
local function setopts(opts, prefix, opts_table)
	if opts_table then
		if type(v) == 'table' then
			C[prefix..k](opts, unpack(v))
		else
			C[prefix..k](opts, v)
		end
	end
	return opts
end


-- rocksdb primitives cache
local rocksdb_txn_cache = {}
local rocksdb_cf_txn_cache = {}
local function rocksdb_txn_gc(t) 
	C.rocksdb_writebatch_clear(t.b)
	table.insert(rocksdb_txn_cache, t)
end
local function rocksdb_txn_new(db)
	local p = table.remove(rocksdb_txn_cache)
	if not p then
	-- if gc metamethod is specified, don't use managed_alloc_typed.
		p = memory.alloc_typed('luact_rocksdb_txn_t')
		p.b = C.rocksdb_writebatch_create();
	end
	p.db = db
	return p
end
local function rocksdb_cf_txn_new(db, cf)
	local p = table.remove(rocksdb_cf_txn_cache)
	if not p then
	-- if gc metamethod is specified, don't use managed_alloc_typed.
		p = memory.alloc_typed('luact_rocksdb_cf_txn_t')
		p.b = C.rocksdb_writebatch_create();
	end
	p.cf = cf
	p.db = db
	return p
end


-- rocksdb options
local rocksdb_o_opts_index, rocksdb_r_opts_index, rocksdb_w_opts_index = {}, {}, {}
local rocksdb_o_opts_mt = { __index = rocksdb_o_opts_index, __gc = C.free }
local rocksdb_r_opts_mt = { __index = rocksdb_r_opts_index, __gc = C.free }
local rocksdb_w_opts_mt = { __index = rocksdb_w_opts_index, __gc = C.free }
function rocksdb_o_opts_index:init(opts)
	setopts(self, "rocksdb_options_set_", opts)
end
function rocksdb_r_opts_index:init(opts)
	setopts(self, "rocksdb_readoptions_set_", opts)
end
function rocksdb_w_opts_index:init(opts)
	setopts(self, "rocksdb_writeoptions_set_", opts)
end
ffi.metatype('rockdb_options_t', rocksdb_o_opts_mt)
ffi.metatype('rockdb_readoptions_t', rocksdb_r_opts_mt)
ffi.metatype('rockdb_writeoptions_t', rocksdb_w_opts_mt)

-- global opts
local open_opts, read_opts, write_opts = 
	C.rocksdb_options_create(), 
	C.rocksdb_readoptions_create(),
	C.rocksdb_writeoptions_create()


-- rocksdb iter
local rocksdb_iter_index = {}
local rocksdb_iter_mt = {
	__index = rocksdb_iter_index,
	__add = rocksdb_iter_add
}
function rocksdb_iter_add(it, n)
	if n > 0 then
		for i=1,n,1 do it:next() end
	else
		for i=1,n,-1 do it:prev() end
	end
end
function rocksdb_iter_index:fin()
	C.rocksdb_iter_destroy(self)
end
function rocksdb_iter_index:valid()
	return C.rocksdb_iter_valid(self) ~= 0
end
function rocksdb_iter_index:first()
	C.rocksdb_iter_seek_to_first(self)
end
function rocksdb_iter_index:last()
	C.rocksdb_iter_seek_to_last(self)
end
function rocksdb_iter_index:next()
	C.rocksdb_iter_next(self)
end
function rocksdb_iter_index:prev()
	C.rocksdb_iter_prev(self)
end
function rocksdb_iter_index:search(k)
	C.rocksdb_iter_seek(k, #k)
end
function rocksdb_iter_index:key()
	return C.rocksdb_iter_key(self, vsz), vsz[0]
end
function rocksdb_iter_index:keystr()
	return ffi.string(self:key())
end
function rocksdb_iter_index:val()
	return C.rocksdb_iter_value(self, vsz), vsz[0]
end
function rocksdb_iter_index:valstr()
	return ffi.string(self:val())
end
function rocksdb_iter_index:error()
	C.rocksdb_iter_get_error(self, errptr)
	return errptr[0]
end
ffi.metatype('rocksdb_iterator_t', rocksdb_iter_mt)


-- rocksdb
local rocksdb_index = {}
local rocksdb_mt = {
	__index = rocksdb_index
}
local vsz = ffi.new('size_t[1]')
function rocksdb_index:column_family(name, opts)
	-- if gc metamethod is specified, don't use managed_alloc_typed.
	local p = memory.alloc_typed('luact_rocksdb_column_family_t')
	p.db = self
	p.cf = callapi(C.rocksdb_create_column_family, self, opts or open_opts, name)
	return p
end
function rocksdb_index:get(k, opts)
	return ffi.string(self:rawget(k, #k, opts))
end
function rocksdb_index:rawget(k, kl, opts)
	return callapi(C.rocksdb_get, self, opts or read_opts, k, kl, vsz), vsz[0]
end
function rocksdb_index:put(k, v, opts)
	return self:rawput(k, #k, v, #v, opts)
end
function rocksdb_index:rawput(k, kl, v, vl, opts)
	return callapi(C.rocksdb_put, self, opts or write_opts, k, kl, v, vl)
end
function rocksdb_index:delete(k, opts)
	return self:rawdelete(k, #k, opts)
end
function rocksdb_index:rawdelete(k, kl, opts)
	return callapi(C.rocksdb_delete, self, opts or write_opts, k, kl)
end
function rocksdb_index:merge(k, v, opts)
	return self:rawmerge(k, #k, v, #v, opts)
end
function rocksdb_index:rawmerge(k, kl, v, vl, opts)
	return callapi(C.rocksdb_merge, ops or write_opts, k, kl, v, vl)
end
function rocksdb_index:new_txn()
	return rocksdb_txn_new(self)
end
local function rocksdb_iter_next(it)
	if it:valid() then
		local k,kl = it:key()
		local v,vl = it:val()
		it:next()
		return k,kl,v,vl
	end
end
local function rocksdb_iter_next_str(it)
	if it:valid() then
		local k = it:keystr()
		local v = it:valstr()
		it:next()
		return k,v
	end
end
function rocksdb_index:iterator(opts)
	return C.rocksdb_create_iterator(self, opts or read_opts)
end
function rocksdb_index:binpairs(opts)
	return rocksdb_iter_next, self:iterator(opts)
end
function rocksdb_index:pairs(opts)
	return rocksdb_iter_next_str, self:iterator(opts)
end
function rocksdb_index:fin()
	C.rocksdb_close(self)
end
ffi.metatype('rocksdb_t', rocksdb_mt)


-- rocksdb column family
local rocksdb_cf_index = util.copy_table(rocksdb_index)
local function rocksdb_cf_gc(t) t:fin() end
local rocksdb_cf_mt = {
	__index = rocksdb_cf_index
	__gc = rocksdb_cf_gc
}
function rocksdb_cf_index:rawget(k, kl, opts)
	return callapi(C.rocksdb_get_cf, self.db, opts or read_opts, self.cf, k, kl, vsz), vsz[0]
end
function rocksdb_cf_index:rawput(k, kl, v, vl, opts)
	return callapi(C.rocksdb_put_cf, self.db, opts or write_opts, self.cf, k, kl, v, vl)
end
function rocksdb_cf_index:rawdelete(k, kl, opts)
	return callapi(C.rocksdb_delete_cf, self.db, opts or write_opts, self.cf, k, kl)
end
function rocksdb_cf_index:rawmerge(k, kl, v, vl, opts)
	return callapi(C.rocksdb_merge_cf, self.db, ops or write_opts, self.cf, k, kl, v, vl)
end
function rocksdb_index:new_txn()
	return rocksdb_cf_txn_new(self.db, self.cf)
end
function rocksdb_cf_index:iterator(opts)
	return C.rocksdb_create_iterator_cf(self.db, opts or read_opts, self.cf)
end
function rocksdb_cf_index:fin()
	callapi(C.rocksdb_drop_column_family, self.db, self.cf)
end
ffi.metatype('luact_rocksdb_column_family_t', rocksdb_cf_mt)


-- rocksdb transaction
local rocksdb_txn_index = util.copy_table(rocksdb_index)
local rocksdb_txn_mt = {
	__index = rocksdb_txn_index,
	__gc = rocksdb_txn_gc,
}
function rocksdb_txn_index:rawput(k, kl, v, vl)
	return C.rocksdb_writebatch_put(self.b, k, kl, v, vl)
end
function rocksdb_txn_index:rawdelete(k, kl)
	return C.rocksdb_writebatch_put(self.b, k, kl)
end
function rocksdb_txn_index:rawmerge(k, kl, v, vl)
	return C.rocksdb_writebatch_merge(self.b, k, kl, v, vl)
end
function rocksdb_txn_index:commit(opts)
	C.rocksdb_write(self.db, opts or write_opts, self.b)
end

local rocksdb_cf_txn_index = util.copy_table(rocksdb_txn_index)
local rocksdb_cf_txn_mt = {
	__index = rocksdb_cf_txn_index,
	__gc = rocksdb_txn_gc,
}
function rocksdb_cf_txn_index:rawput(k, kl, v, vl)
	return C.rocksdb_writebatch_put_cf(self.b, self.cf, k, kl, v, vl)
end
function rocksdb_cf_txn_index:rawdelete(k, kl)
	return C.rocksdb_writebatch_put_cf(self.b, self.cf, k, kl)
end
function rocksdb_cf_txn_index:rawmerge(k, kl, v, vl)
	return C.rocksdb_writebatch_merge_cf(self.b, self.cf, k, kl, v, vl)
end



-- module interface
function _M.open(path, opts)
	return callapi(C.rocksdb_open, opts or open_opts, path)
end
-- you can change global options only once by using init
_M.open_options, _M.read_options, _M.write_options = open_opts, read_opts, write_opts
-- create independent option instance
function _M.new_open_opts(opts_table)
	local opts = C.rocksdb_options_create()
	opts:init(opts_table)
	return opts
end
function _M.new_read_opts(opts_table)
	local opts = C.rocksdb_readoptions_create()
	opts:init(opts_table)
	return opts
end
function _M.new_write_opts(opts_table)
	local opts = C.rocksdb_writeoptions_create()
	opts:init(opts_table)
	return opts
end
function _M.close(db)
	db:fin()
end

return _M