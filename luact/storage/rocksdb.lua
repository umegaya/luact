local ffi = require 'ffiex.init'
local util = require 'pulpo.util'
local memory = require 'pulpo.memory'
local thread = require 'pulpo.thread'
local exception = require 'pulpo.exception'
local wp = require 'pulpo.debug.watchpoint'
exception.define('rocksdb')

local _M = {}
local dbpath_map = {}

-- cdefs 
local C = ffi.C
ffi.cdef[[
#include <rocksdb/c.h>
typedef struct luact_rocksdb_column_family {
	rocksdb_t *db;
	rocksdb_column_family_handle_t *cf;
} luact_rocksdb_column_family_t;
typedef struct luact_rocksdb_cf_handle {
	int32_t refc;
	rocksdb_column_family_handle_t *cf;
} luact_rocksdb_cf_handle_t;
typedef struct luact_rocksdb {
	rocksdb_t *db;
	int32_t refc;
	size_t cfsize, cfused;
	const char **names;
	luact_rocksdb_cf_handle_t *handles;
} luact_rocksdb_t;
typedef struct luact_rocksdb_txn {
	rocksdb_t *db;
	rocksdb_writebatch_t *b;
} luact_rocksdb_txn_t;
]]
local LIB = ffi.load('rocksdb')

-- local functions
local errptr = ffi.new('char*[1]')
local function callapi(fn, ...)
	-- I really wanna append errptr, but it causes only first element of ... is passed to fn.
	-- so errptr is given at the caller of callapi :< its really bad.
	-- TODO : if this is critical for execution speed, give errptr at the caller side of callapi.
	local tmp = {...}
	table.insert(tmp, errptr)
	errptr[0] = util.NULL
	local r = fn(unpack(tmp)) 
	if errptr[0] ~= util.NULL then
		exception.raise('rocksdb', 'api_error', ffi.string(errptr[0]))
	end
	return r
end
local function setopts(opts, prefix, opts_table)
	if opts_table then
		for k,v in pairs(opts_table) do
			if type(v) == 'table' then
				LIB[prefix..k](opts, unpack(v))
			else
				LIB[prefix..k](opts, v)
			end
		end
	end
	return opts
end
local function shmem_key(path)
	return 'rocksdb:'..path
end
local function db_from_key(db)
	for k,v in pairs(dbpath_map) do
		-- print('dbpath_map:', k, v, db)
		if v == db then
			return k
		end
	end
end



-- rocksdb primitives cache
local rocksdb_txn_cache = {}
local rocksdb_cf_txn_cache = {}
local function rocksdb_txn_gc(t) 
	LIB.rocksdb_writebatch_clear(t.b)
	table.insert(rocksdb_txn_cache, t)
end
local function rocksdb_txn_new(db)
	local p = table.remove(rocksdb_txn_cache)
	if not p then
	-- if gc metamethod is specified, don't use managed_alloc_typed.
		p = memory.alloc_typed('luact_rocksdb_txn_t')
		p.b = LIB.rocksdb_writebatch_create();
	end
	p.db = db
	return p
end


-- rocksdb options
local rocksdb_o_opts_index, rocksdb_r_opts_index, rocksdb_w_opts_index = {}, {}, {}
local rocksdb_o_opts_mt = { __index = rocksdb_o_opts_index, __gc = LIB.rocksdb_options_destroy }
local rocksdb_r_opts_mt = { __index = rocksdb_r_opts_index, __gc = LIB.rocksdb_readoptions_destroy }
local rocksdb_w_opts_mt = { __index = rocksdb_w_opts_index, __gc = LIB.rocksdb_writeoptions_destroy }
function rocksdb_o_opts_index:init(opts)
	setopts(self, "rocksdb_options_set_", opts)
end
function rocksdb_r_opts_index:init(opts)
	setopts(self, "rocksdb_readoptions_set_", opts)
end
function rocksdb_w_opts_index:init(opts)
	setopts(self, "rocksdb_writeoptions_set_", opts)
end
ffi.metatype('rocksdb_options_t', rocksdb_o_opts_mt)
ffi.metatype('rocksdb_readoptions_t', rocksdb_r_opts_mt)
ffi.metatype('rocksdb_writeoptions_t', rocksdb_w_opts_mt)

-- global opts
local open_opts, read_opts, write_opts = 
	LIB.rocksdb_options_create(), 
	LIB.rocksdb_readoptions_create(),
	LIB.rocksdb_writeoptions_create()
-- add default settings
open_opts:init({
	create_if_missing = 1, 
})


-- luact_rocksdb
local luact_rocksdb_index = {}
local luact_rocksdb_mt = {
	__index = luact_rocksdb_index,
}
function luact_rocksdb_index:fin()
	for i=0,tonumber(self.cfused)-1 do
		if self.handles[i].refc > 0 then
			logger.warn('rocksdb', 'column_family leak', self.names[i])
		end
		-- only when database itself finished, column family can destroy.
		-- otherwise we never be able to re-create it.
		memory.free(ffi.cast('void *', self.names[i]))
		LIB.rocksdb_column_family_handle_destroy(self.handles[i].cf)
	end
	if self.names ~= util.NULL then
		memory.free(self.names)
	end
	if self.handles ~= util.NULL then
		memory.free(self.handles)
	end
	if self.db ~= util.NULL then
		self.db:fin()
	end
end
function luact_rocksdb_index:close_column_family(cf, destroy)
	for i=0,tonumber(self.cfused)-1 do
		if (self.handles[i].cf == cf) then
			if destroy then
				-- mark when database closed, this column family destroyed
				callapi(LIB.rocksdb_drop_column_family, self.db, cf)
			end
			self.handles[i].refc = self.handles[i].refc - 1
			return true
		end
	end
	return found
end
function luact_rocksdb_index:destroy_column_family(cf)
	return self:close_column_family(cf, true)
end
function luact_rocksdb_index:reserve_cf_entry(size)
	local required = self.cfused + size
	while required > self.cfsize do
		self.cfsize = self.cfsize * 2
	end
	local new_names, new_handles = 
		memory.realloc_typed('const char *', self.names, self.cfsize), 
		memory.realloc_typed('luact_rocksdb_cf_handle_t', self.handles, self.cfsize)
	if new_names == util.NULL or new_handles == util.NULL then
		exception.raise('fatal', 'rocksdb memory allocation error', new_names, new_handles)
	end
	self.names = new_names
	self.handles = new_handles
end
function luact_rocksdb_index:open_column_family(name, opts)
	for i=0,tonumber(self.cfused)-1 do
		if util.strcmp(self.names[i], name, #name) then
			self.handles[i].refc = self.handles[i].refc + 1
			return self.handles[i].cf
		end
	end
	self:reserve_cf_entry(1)
	local cf = callapi(LIB.rocksdb_create_column_family, self.db, opts, name)
	local r = self.handles[self.cfused]
	self.names[self.cfused] = memory.strdup(name)
	r.cf = cf
	r.refc = 1
	self.cfused = self.cfused + 1
	return cf
end
ffi.metatype('luact_rocksdb_t', luact_rocksdb_mt)

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
	LIB.rocksdb_iter_destroy(self)
end
function rocksdb_iter_index:valid()
	return LIB.rocksdb_iter_valid(self) ~= 0
end
function rocksdb_iter_index:first()
	LIB.rocksdb_iter_seek_to_first(self)
end
function rocksdb_iter_index:last()
	LIB.rocksdb_iter_seek_to_last(self)
end
function rocksdb_iter_index:next()
	LIB.rocksdb_iter_next(self)
end
function rocksdb_iter_index:prev()
	LIB.rocksdb_iter_prev(self)
end
function rocksdb_iter_index:search(k)
	LIB.rocksdb_iter_seek(k, #k)
end
function rocksdb_iter_index:key()
	return LIB.rocksdb_iter_key(self, vsz), vsz[0]
end
function rocksdb_iter_index:keystr()
	return ffi.string(self:key())
end
function rocksdb_iter_index:val()
	return LIB.rocksdb_iter_value(self, vsz), vsz[0]
end
function rocksdb_iter_index:valstr()
	return ffi.string(self:val())
end
function rocksdb_iter_index:error()
	LIB.rocksdb_iter_get_error(self, errptr)
	return errptr[0]
end
ffi.metatype('rocksdb_iterator_t', rocksdb_iter_mt)


-- rocksdb
local rocksdb_index = {}
local rocksdb_mt = {
	__index = rocksdb_index
}
local vsz = ffi.new('size_t[1]')
local wpset
function rocksdb_index:column_family(name, opts)
	local key = db_from_key(self)
	-- decrement ref count
	local cf = thread.lock_shared_memory(key, function (ptr, dbname, options)
		local p = ffi.cast('luact_rocksdb_t*', ptr)
		return p:open_column_family(dbname, options)
	end, name, opts or open_opts)
	local p = memory.managed_alloc_typed('luact_rocksdb_column_family_t')
	p.cf = cf
	p.db = self
	return p
end
function rocksdb_index:get(k, opts)
	local v, vl = self:rawget(k, #k, opts)
	local s = ffi.string(v, vl)
	memory.free(v)
	return s
end
function rocksdb_index:rawget(k, kl, opts)
	return callapi(LIB.rocksdb_get, self, opts or read_opts, k, kl, vsz), vsz[0]
end
function rocksdb_index:put(k, v, opts)
	return self:rawput(k, #k, v, #v, opts)
end
function rocksdb_index:rawput(k, kl, v, vl, opts)
	return callapi(LIB.rocksdb_put, self, opts or write_opts, k, kl, v, vl)
end
function rocksdb_index:delete(k, opts)
	return self:rawdelete(k, #k, opts)
end
function rocksdb_index:rawdelete(k, kl, opts)
	return callapi(LIB.rocksdb_delete, self, opts or write_opts, k, kl)
end
function rocksdb_index:merge(k, v, opts)
	return self:rawmerge(k, #k, v, #v, opts)
end
function rocksdb_index:rawmerge(k, kl, v, vl, opts)
	return callapi(LIB.rocksdb_merge, ops or write_opts, k, kl, v, vl)
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
	return LIB.rocksdb_create_iterator(self, opts or read_opts)
end
function rocksdb_index:binpairs(opts)
	return rocksdb_iter_next, self:iterator(opts)
end
function rocksdb_index:pairs(opts)
	return rocksdb_iter_next_str, self:iterator(opts)
end
function rocksdb_index:close()
	local key = db_from_key(self)
	print(key)
	-- decrement ref count
	local cnt = thread.lock_shared_memory(key, function (ptr)
		local p = ffi.cast('luact_rocksdb_t*', ptr)
		p.refc = p.refc - 1
		return p.refc
	end)
	if cnt <= 0 then
		-- remove from shared memory
		dbpath_map[key] = nil
		thread.shared_memory(key, nil)
	end
end
function rocksdb_index:fin()
	LIB.rocksdb_close(self)
end
ffi.metatype('rocksdb_t', rocksdb_mt)


-- rocksdb column family
local rocksdb_cf_index = util.copy_table(rocksdb_index)
rocksdb_cf_index.close = nil
local function rocksdb_cf_gc(t) t:fin() end
local rocksdb_cf_mt = {
	__index = rocksdb_cf_index,
}
function rocksdb_cf_index:rawget(k, kl, opts)
	return callapi(LIB.rocksdb_get_cf, self.db, opts or read_opts, self.cf, k, kl, vsz), vsz[0]
end
function rocksdb_cf_index:rawput(k, kl, v, vl, opts)
	return callapi(LIB.rocksdb_put_cf, self.db, opts or write_opts, self.cf, k, kl, v, vl)
end
function rocksdb_cf_index:rawdelete(k, kl, opts)
	return callapi(LIB.rocksdb_delete_cf, self.db, opts or write_opts, self.cf, k, kl)
end
function rocksdb_cf_index:rawmerge(k, kl, v, vl, opts)
	return callapi(LIB.rocksdb_merge_cf, self.db, ops or write_opts, self.cf, k, kl, v, vl)
end
function rocksdb_cf_index:iterator(opts)
	return LIB.rocksdb_create_iterator_cf(self.db, opts or read_opts, self.cf)
end
function rocksdb_cf_index:new_txn()
	return rocksdb_txn_new(self.db)
end
function rocksdb_cf_index:fin()
	local key = db_from_key(self.db)
	-- decrement ref count
	thread.lock_shared_memory(key, function (ptr, cf)
		local p = ffi.cast('luact_rocksdb_t*', ptr)
		p:close_column_family(cf)
	end, self.cf)
end
function rocksdb_cf_index:destroy()
	local key = db_from_key(self.db)
	-- decrement ref count
	thread.lock_shared_memory(key, function (ptr, cf)
		local p = ffi.cast('luact_rocksdb_t*', ptr)
		p:destroy_column_family(cf)
	end, self.cf)
	callapi(LIB.rocksdb_drop_column_family, self.db, self.cf)
end
ffi.metatype('luact_rocksdb_column_family_t', rocksdb_cf_mt)


-- rocksdb transaction
local rocksdb_txn_index = util.copy_table(rocksdb_index)
local rocksdb_txn_mt = {
	__index = rocksdb_txn_index,
	__gc = rocksdb_txn_gc,
}
function rocksdb_txn_index:rawput(k, kl, v, vl)
	return LIB.rocksdb_writebatch_put(self.b, k, kl, v, vl)
end
function rocksdb_txn_index:rawdelete(k, kl)
	return LIB.rocksdb_writebatch_delete(self.b, k, kl)
end
function rocksdb_txn_index:rawmerge(k, kl, v, vl)
	return LIB.rocksdb_writebatch_merge(self.b, k, kl, v, vl)
end
function rocksdb_txn_index:put_cf(cf, k, v)
	return self:rawput_cf(cf, k, #k, v, #v)
end
function rocksdb_txn_index:rawput_cf(cf, k, kl, v, vl)
	return LIB.rocksdb_writebatch_put_cf(self.b, cf.cf, k, kl, v, vl)
end
function rocksdb_txn_index:delete_cf(cf, k)
	return self:rawdelete_cf(cf, k, #k)
end
function rocksdb_txn_index:rawdelete_cf(cf, k, kl)
	return LIB.rocksdb_writebatch_delete_cf(self.b, cf.cf, k, kl)
end
function rocksdb_txn_index:merge_cf(cf, k, v)
	return self:rawmerge_cf(cf, k, #k, v, #v)
end
function rocksdb_txn_index:rawmerge_cf(cf, k, kl, v, vl)
	return LIB.rocksdb_writebatch_merge_cf(self.b, cf.cf, k, kl, v, vl)
end
function rocksdb_txn_index:fin()
	rocksdb_txn_gc(self)
end
function rocksdb_txn_index:commit(opts)
	callapi(LIB.rocksdb_write, self.db, opts or write_opts, self.b)
end
ffi.metatype('luact_rocksdb_txn_t', rocksdb_txn_mt)


-- module interface
function _M.open(name, opts, debug_conf)
	local key = shmem_key(name)
	local ldb = thread.shared_memory(key, function ()
		debug_conf = debug_conf or {}
		opts = opts or open_opts
		local size_p = ffi.new('size_t[1]')
		local ok, r = pcall(callapi, LIB.rocksdb_list_column_families, opts, name, size_p)
		local mem = memory.alloc_fill_typed('luact_rocksdb_t')
		if ok then
			mem.names = ffi.cast('const char **', r)
			mem.cfsize = size_p[0]
			local handles = ffi.new('rocksdb_column_family_handle_t*[?]', mem.cfsize)
			local options_p = ffi.new('const rocksdb_options_t*[?]', mem.cfsize)
			for i=0,tonumber(mem.cfsize)-1 do
				options_p[i] = opts
			end
			ok, r = pcall(callapi, LIB.rocksdb_open_column_families, 
				opts, name, mem.cfsize, mem.names, options_p, handles)
			if not ok then
				mem:fin()
				error(r)
			end
			mem.handles = memory.alloc_fill_typed('luact_rocksdb_cf_handle_t', mem.cfsize)
			for i=0,tonumber(mem.cfsize)-1 do
				mem.handles[i].cf = handles[i]
				mem.handles[i].refc = 0
			end
			mem.cfused = mem.cfsize
			mem.db = r
			return 'luact_rocksdb_t', mem
		else
			if (type(r) ~= 'table') or (not r:get_arg(2):match('No such file or directory')) then 
				mem:fin()
				error(r) 
			end
			ok, r = pcall(callapi, LIB.rocksdb_open, opts, name)
			if not ok then
				mem:fin()
				error(r)
			end	
			mem.cfsize = debug_conf.initial_column_family_buffer_size or 16
			mem.cfused = 0
			mem.names = memory.alloc_fill_typed('const char *', mem.cfsize)
			mem.handles = memory.alloc_fill_typed('luact_rocksdb_cf_handle_t', mem.cfsize)
			mem.db = r
			return 'luact_rocksdb_t', mem
		end
	end)
	thread.lock_shared_memory(key, function (ptr)
		local mem = ffi.cast('luact_rocksdb_t*', ptr)
		mem.refc = mem.refc + 1
	end)
	local db = ldb.db
	dbpath_map[key] = db
	return db
end
-- you can change global options only once by using init
_M.open_options, _M.read_options, _M.write_options = open_opts, read_opts, write_opts
-- create independent option instance
function _M.new_open_opts(opts_table)
	local opts = LIB.rocksdb_options_create()
	opts:init(opts_table)
	return opts
end
function _M.new_read_opts(opts_table)
	local opts = LIB.rocksdb_readoptions_create()
	opts:init(opts_table)
	return opts
end
function _M.new_write_opts(opts_table)
	local opts = LIB.rocksdb_writeoptions_create()
	opts:init(opts_table)
	return opts
end
function _M.close(db)
	db:fin()
end

return _M
