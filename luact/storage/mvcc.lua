--[[
	manages multiple value for single key by using versioned key( = keyname + lamport style logical timestamp )
	most part of initial code is from cockroachDB's mvcc.go. thanks to cockroach authors for well documented codes :D
]]
local ffi = require 'ffiex.init'

local txncoord = require 'luact.storage.txncoord'
local clock = require 'luact.clock'
local buffer = require 'luact.util.buffer'
local lamport = require 'pulpo.lamport'
local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'

local _M = {}
local mergers = {}

-- cdef
ffi.cdef [[
typedef enum luact_mvcc_key_type {
	MVCC_KEY_VALUE,
	MVCC_KEY_VERSIONED,
} luact_mvcc_key_type_t;

typedef struct luact_mvcc_bytes_codec {
	uint16_t size, idx;
	uint32_t *lengths;
	char **buffers;
} luact_mvcc_bytes_codec_t;

typedef struct luact_mvcc_stats {
	size_t bytes_key, bytes_val;
	size_t n_key, n_val;
	size_t uncommitted_bytes;
	uint64_t last_update;
} luact_mvcc_stats_t;

typedef struct luact_mvcc_metadata {
	pulpo_hlc_t timestamp;
	uint32_t key_len, val_len;
	luact_dht_txn_t txn;
	uint8_t delete_flag, reserved[3];
} luact_mvcc_metadata_t;

typedef struct luact_mvcc_merge_cas {
	char *prev_val; //if NULL, swap success, if ~= NULL, previous value in this key
	size_t prev_val_len; //length of prev_val
	bool success;
	uint32_t cl, sl; //compare data/swap data length
	char p[0];
} luact_mvcc_merge_cas_t;
]]
local MVCC_KEY_VALUE = ffi.cast('luact_mvcc_key_type_t', 'MVCC_KEY_VALUE')
local MVCC_KEY_VERSIONED = ffi.cast('luact_mvcc_key_type_t', 'MVCC_KEY_VERSIONED')



-- exception
exception.define('mvcc')



-- local functions
local function pstr(k, kl)
	return ('%q'):format(ffi.string(k, kl))
end
local function traverse_iter(iter, cb)
	iter:first()
	while iter:valid() do
		cb(iter)
		iter:next()
	end
end
local function dump_db(db)
	print('-- iterate keys')
	traverse_iter(db:iterator(), function (iter) _M.dump_key(iter:key()) end)
	print('-- end iterate keys')
end


-- system merger 
-- cas
local merger_cas_mt = {}
local merger_cas_cache 
merger_cas_mt.__index = merger_cas_mt
function merger_cas_mt.size(cl, sl)
	return ffi.sizeof('luact_mvcc_merge_cas_t') + cl + sl
end
function merger_cas_mt.new(compare, swap, cl, sl)
	local p
	cl = cl or (compare and #compare or 0)
	sl = sl or #swap
	local required = merger_cas_mt.size(cl, sl)
	if merger_cas_cache then
		local size = #merger_cas_cache
		if size < required then
			while size < required do
				size = size * 2
			end
			local tmp = ffi.cast('luact_mvcc_merge_cas_t*', memory.realloc(merger_cas_cache, size))
			if tmp == ffi.NULL then
				exception.raise('fatal', 'realloc', size, merger_cas_cache)
			end
			merger_cas_cache = tmp
		end
	else
		merger_cas_cache = ffi.cast('luact_mvcc_merge_cas_t*', memory.alloc(required))
	end
	p = merger_cas_cache
	p.cl = cl
	p.prev_val = ffi.NULL
	p.prev_val_len = 0
	if cl > 0 then
		ffi.copy(p:compare(), compare, cl)
	end
	ffi.copy(p:swap(), swap, sl)
	p.sl = sl
	return p
end
_M.op_cas = merger_cas_mt.new
function merger_cas_mt:compare()
	return self.cl > 0 and self.p or nil
end
function merger_cas_mt:swap()
	return self.p + self.cl
end
function merger_cas_mt:__len()
	return merger_cas_mt.size(self.cl, self.sl)
end
-- true : success
-- false + string : value which causes failure
-- false + false : failure in cas with non-exist value
function merger_cas_mt:result()
	if self.prev_val_len > 0 then
		return self.success, ffi.string(self.prev_val, self.prev_val_len)
	else
		return self.success
	end
end
function merger_cas_mt.process(key, key_length, 
					existing, existing_length, 
					payload, payload_length,
					new_value_length)
	local context = ffi.cast('luact_mvcc_merge_cas_t*', payload)
	local cmp = context:compare()
	--logger.warn('cas merger', payload, ffi.string(existing, existing_length), cmp and ffi.string(cmp, context.cl) or "nil")
	if not existing_length then
		if cmp == nil then
			new_value_length[0] = context.sl
			context.prev_val_len = 0
			context.success = true
			return context:swap(), true
		end
	elseif cmp and context.cl == existing_length and 
		memory.cmp(existing, cmp, existing_length) then
		new_value_length[0] = context.sl
		--logger.info('casres intenal', context.result)
		context.prev_val = existing
		context.prev_val_len = existing_length
		context.success = true
		return context:swap(), true
	end
	context.prev_val = existing
	context.prev_val_len = existing_length
	context.success = false
end
ffi.metatype('luact_mvcc_merge_cas_t', merger_cas_mt)



-- mvcc metadata
local mvcc_meta_mt = {}
mvcc_meta_mt.__index = mvcc_meta_mt
function mvcc_meta_mt:__len()
	return ffi.sizeof('luact_mvcc_metadata_t')
end
function mvcc_meta_mt:inline()
	return false
end
function mvcc_meta_mt:deleted()
	return self.delete_flag ~= 0
end
function mvcc_meta_mt:set_deleted(on)
	self.delete_flag = on and 1 or 0
end
function mvcc_meta_mt:set_inline_value(v, vl)
	self.delete_flag = 0
end
function mvcc_meta_mt:set_txn(txn)
	if txn then
		ffi.copy(self.txn, txn, ffi.sizeof(self.txn))
	else
		self.txn:invalidate()
	end
end
function mvcc_meta_mt:set_current_kv_len(kl, vl)
	self.key_len, self.val_len = kl, vl
end
ffi.metatype('luact_mvcc_metadata_t', mvcc_meta_mt)



-- mvcc stat
local mvcc_stats_mt = {}
mvcc_stats_mt.__index = mvcc_stats_mt
function mvcc_stats_mt:init()
	self.bytes_key = 0
	self.bytes_val = 0
	self.n_key = 0
	self.n_val = 0
	self.uncommitted_bytes = 0
	self.last_update = 0
end
function mvcc_stats_mt:updated()
	local s,us = util.clock_pair()
	self.last_update = s * 1000 * 1000 + us
end
function mvcc_stats_mt:inline(kl, prev_meta, meta, already_exists)
	if already_exists then
		-- kl already added, so ignore
		self.bytes_key = self.bytes_key - prev_meta.key_len + meta.key_len
		self.bytes_val = self.bytes_val - prev_meta.val_len + meta.val_len
	else
		self.bytes_key = self.bytes_key + meta.key_len + kl
		self.bytes_val = self.bytes_val + meta.val_len + ffi.sizeof(prev_meta)
	end
	self:updated()
end
function mvcc_stats_mt:put(kl, prev_meta, meta, already_exists)
	-- print('put', self.bytes_key, self.bytes_val, prev_meta.key_len, meta.key_len, kl)
	if already_exists then
		if prev_meta.txn:valid() or (prev_meta.timestamp == meta.timestamp) then
			-- data which is written when prev_meta is created, has removed.
			-- or if old/new data has same timestamp, old data will be overwritten. 
			self.bytes_key = self.bytes_key - prev_meta.key_len + meta.key_len
			self.bytes_val = self.bytes_val - prev_meta.val_len + meta.val_len			
		else
			-- new uncommitted value is written. just add value
			self.bytes_key = self.bytes_key + meta.key_len
			self.bytes_val = self.bytes_val + meta.val_len			
		end
	else
		assert(not prev_meta.txn:valid())
		self.bytes_key = self.bytes_key + meta.key_len + kl
		self.bytes_val = self.bytes_val + meta.val_len + ffi.sizeof(prev_meta)	
	end
	-- print(' ====> ', self.bytes_key, self.bytes_val)
	self:updated()
end
function mvcc_stats_mt:committed(kl, prev_meta, meta)
	-- committed just update metadata and move versioned key/value to another key/value (not length change)
	-- so do nothing.
	self:updated()
end
function mvcc_stats_mt:aborted(kl, prev_meta, meta, prev_key_iter)
	-- print('abort', self.bytes_key, self.bytes_val)
	-- uncommitted value has removed
	self.bytes_key = self.bytes_key - prev_meta.key_len
	self.bytes_val = self.bytes_val - prev_meta.val_len
	if not prev_key_iter then
		-- no committed value for this key, so metadata itself has removed
		self.bytes_key = self.bytes_key - kl
		self.bytes_val = self.bytes_val - ffi.sizeof(prev_meta)
	end
	-- print(' ====> ', self.bytes_key, self.bytes_val)
	self:updated()
end
function mvcc_stats_mt:gc(kl, vl)
	-- print('self:gc', self.bytes_key, self.bytes_key - kl, self.bytes_val, self.bytes_val - vl)
	self.bytes_key = self.bytes_key - kl
	self.bytes_val = self.bytes_val - vl
end
ffi.metatype('luact_mvcc_stats_t', mvcc_stats_mt)



-- mvcc key
local mvcc_bytes_codec_mt = {}
mvcc_bytes_codec_mt.__index = mvcc_bytes_codec_mt
function mvcc_bytes_codec_mt:init(size)
	self.size = size
	self.idx = 0
	self.lengths = memory.alloc_fill_typed('uint32_t', size)
	self.buffers = memory.alloc_fill_typed('char *', size)
	for i=0, self.size - 1 do
		self.lengths[i] = 256
		self.buffers[i] = memory.alloc(self.lengths[i])
	end
end
function mvcc_bytes_codec_mt:reserve(k, kl, ts)
	-- +1 for using in self:next_of
	local reqlen = (util.encode_binary_length(kl + (ts and ffi.sizeof('pulpo_hlc_t') or 0)) + 1)
	local curidx = self.idx
	local len = self.lengths[curidx]
	if len >= reqlen then 
		self.idx = (self.idx + 1) % self.size
		return self.buffers[curidx], len 
	end
	while len < reqlen do
		len = len * 2
	end
	-- print('---------------- reserve: newlen', len, kl, ffi.sizeof('pulpo_hlc_t'))
	local tmp = memory.realloc_typed('char', self.buffers[curidx], len)
	if tmp ~= ffi.NULL then
		self.buffers[curidx] = tmp
		self.lengths[curidx] = len
		self.idx = (self.idx + 1) % self.size
		return tmp, len
	else
		exception.raise('fatal', 'malloc', len)
	end
end
function mvcc_bytes_codec_mt:encode(k, kl, ts)
	local _, olen, ofs
	local p, len = self:reserve(k, kl, ts)
	_, ofs = util.encode_binary(k, kl, p, len)
	if ts then
		_, olen = util.encode_binary(ts.p, ffi.sizeof(ts), p + ofs, len - ofs)
		ofs = ofs + olen
	end
	return p, ofs
end
function mvcc_bytes_codec_mt:decode(ek, ekl)
	local p, len = self:reserve(ek, ekl, true)
	local k, kl, n_read = util.decode_binary(ek, ekl, p, len)
	if n_read >= ekl then
		return k, kl
	else
		-- +1 for additional \0 when this pointer called with next_of
		local ts, tsl = util.decode_binary(ek + n_read, ekl - n_read, p + kl + 1, len - kl - 1)
		return k, kl, ffi.cast('pulpo_hlc_t *', ts)
	end
end
function mvcc_bytes_codec_mt:available(p, pl)
	for i=0, self.size - 1 do
		if self.buffers[i] == p then
			if (self.lengths[i] - pl) > 0 then
				return true
			else
				logger.report('error', 'allocated pointer length short: try recover')
				local tmp = memory.realloc_typed('char', p, pl)
				if tmp == ffi.NULL then
					exception.raise('fatal', 'realloc', pl, p)
				end
				self.buffers[i] = tmp
				return true
			end
		end
	end
end
-- caluculate next key by appending \0 to last 
function mvcc_bytes_codec_mt:next_of(k, kl)
	if self:available(k, kl) then
		k[kl] = 0
		return k, kl + 1
	else
		local p, len = self:reserve(k, kl)
		ffi.copy(p, k, kl)
		return self:next_of(p, kl)
	end
end
function mvcc_bytes_codec_mt:upper_bound_of_prefix(k, kl)
	local rk, rkl = self:reserve(k, kl)
	ffi.copy(rk, k, kl)
	for i=kl-1,0,-1 do
		rk[i] = rk[i] + 1
		if rk[i] ~= 0 then
			return rk, rkl
		end
	end
	return rk, rkl
end
function mvcc_bytes_codec_mt:timestamp_of(iter)
	local k, kl, ts = self:decode(iter:key())
	return ts
end
ffi.metatype('luact_mvcc_bytes_codec_t', mvcc_bytes_codec_mt)



-- mvcc ctype
local mvcc_mt = {}
function mvcc_mt:init(db)
	self.db = db
end
function mvcc_mt:fin()
	self.db:fin()
end
function mvcc_mt:column_family(name, opts)
	exception.raise('mvcc', 'not_support', 'please implement it properly for each kind of mvcc')
end
function mvcc_mt:default_filter(k, kl, v, vl, ts, txn, opts, n, results)
	local value, value_len, value_ts = self:rawget_internal(k, kl, v, vl, ts, txn, opts)
	-- print('filter', pstr(v, vl), ts, value, value_len)
	if value then
		-- print(n, pstr(k, kl), pstr(value, value_len))
		if type(n) == 'number' then
			local ks = ffi.string(k, kl)
			local vs = ffi.string(value, value_len)
			table.insert(results, {ks, #ks, vs, #vs, value_ts})
			if n > 0 and #results >= n then
				return true
			end
		elseif type(n) == 'function' then
			local ok, r = pcall(n, k, kl, value, value_len, value_ts, results)
			if ok then
				return r
			else
				memory.free(value)
				error(r)
			end
		end
	end
end
function mvcc_mt:committed_filter(k, kl, v, vl, fn, options, it)
	local meta = ffi.cast('luact_mvcc_metadata_t*', v)
	local ck, ckl, ts = _M.bytes_codec:encode(k, kl, meta.timestamp)
	if meta.txn:valid() then
		-- the value corresponding to cvk, cvkl may not committed. 
		-- find (key, len) which satisfied following condition:
		-- (k, kl) < (key, len) < (ck, ckl)
		local ek, ekl = _M.bytes_codec:encode(k, kl)
		it:seek(ck, ckl)
		while it:valid() do
			it:prev()
			if memory.rawcmp_ex(ek, ekl, it:key()) >= 0 then
				break -- no committed value. skip to next key
			end
			if memory.rawcmp_ex(ck, ckl, it:key()) > 0 then
				local cv, cvl = it:val()
				if cvl > 0 then
					ck, ckl, ts = _M.bytes_codec:decode(it:key())
					fn(ck, ckl, cv, cvl, ts)
					break
				end
			end
		end
	else
		local cv, cvl = self.db:rawget(ck, ckl, options)
		if cv then
			ck, ckl, ts = _M.bytes_codec:decode(ck, ckl)
			fn(ck, ckl, cv, cvl, ts)
		end
	end
end
function mvcc_mt:scan_internal(s, sl, e, el, n, boundary, ts, txn, opts)
	local results = {}
	local it = self:rawscan_internal(s, sl, e, el, opts, self.default_filter, boundary, ts, txn, opts, n, results)
	if #results > 0 then 
		-- hold iterator to retain memory block from it
		results[0] = it
	end
	return results -- after results gc'ed, it will be freed.
end
function mvcc_mt:scan(s, sl, e, el, n, ts, txn, opts)
	return self:scan_internal(s, sl, e, el, n, mvcc_mt.break_if_ge, ts, txn, opts)
end
function mvcc_mt:scan_committed(s, sl, e, el, cb, opts)
	local iter = self.db:iterator(opts)
	self:rawscan(s, sl, e, el, opts, self.committed_filter, cb, opts, iter)
end
-- scan and apply iterator for the meta key in {(s, sl) <= {key} < {e, el}} and its value.
function mvcc_mt:rawscan_internal(s, sl, e, el, opts, cb, boundary, ...)
	--dump_db(self.db)
	local it = self.db:iterator(opts)
	it:seek(_M.bytes_codec:encode(s, sl)) --> seek to the smallest of bigger key
	while it:valid() do
		local k, kl, ts = _M.bytes_codec:decode(it:key())
		-- logger.warn('rawscan_internal', pstr(k, kl), pstr(e, el), ts, kl, el)
		if ts then
			exception.raise('mvcc', 'invalid_key', 'start key should not be versioned key', pstr(s, sl), ts)
		end
		-- break if exceed end boundary
		if boundary(e, el, k, kl) then
			break
		end
		local v, vl = it:val()
		if cb(self, k, kl, v, vl, ...) == true then
			break
		end
		k, kl = _M.bytes_codec:next_of(k, kl)
		it:seek(_M.bytes_codec:encode(k, kl)) -- effectively skip all versioned key
	end
	return it
end
function mvcc_mt.break_if_ge(e, el, k, kl)
	return memory.rawcmp_ex(e, el, k, kl) <= 0
end
function mvcc_mt.break_if_gt(e, el, k, kl)
	return memory.rawcmp_ex(e, el, k, kl) < 0
end
function mvcc_mt:rawscan(s, sl, e, el, opts, cb, ...)
	return self:rawscan_internal(s, sl, e, el, opts, cb, mvcc_mt.break_if_ge, ...)
end
function mvcc_mt:read_kv_filter(k, kl, v, vl, n, results)
	if type(n) == 'number' then
		local ks = ffi.string(k, kl)
		local vs = ffi.string(v, vl)
		table.insert(results, {ks, #ks, vs, #vs})
		if n > 0 and #results >= n then
			return true
		end
	elseif type(n) == 'function' then
		return n(k, kl, v, vl)
	end
end
function mvcc_mt:scan_all(s, e, n, opts)
	local results = {}
	local it = self:rawscan_all(s, #s, e, #e, opts, self.read_kv_filter, n, results)
	if #results > 0 then
		-- holds iterator to retain memory block from it
		results[0] = it
	end
	return results
end
-- scan and apply iterator for all key in {(s, sl) <= {key} < {e, el}} and its value.
function mvcc_mt:rawscan_all(s, sl, e, el, opts, cb, ...)
	local it = self.db:iterator(opts)
	-- print('-- iterate keys'); traverse_iter(it, function (iter) _M.dump_key(iter:key()) end); print('-- end iterate keys')
	it:seek(_M.bytes_codec:encode(s, sl)) --> seek to the smallest of bigger key
	while it:valid() do
		local k, kl = it:key()
		local ek, ekl, ts = _M.bytes_codec:decode(k, kl)
		-- break if exceed end boundary
		if memory.rawcmp_ex(e, el, ek, ekl) <= 0 then
			break
		end
		local v, vl = it:val()
		if cb(self, k, kl, v, vl, ...) == true then
			break
		end
		it:next()
	end
	return it
end
-- seek first key which is from_k, from_kl <=/< (key) <=/< k, kl
-- (reject_boundry false/true, respectively)
function mvcc_mt:seek_prev(k, kl, from_k, from_kl, reject_boundary)
	local it = self.db:iterator()
	it:seek(k, kl)
	if not it:valid() then
		it:last()
	elseif reject_boundary then
		if memory.rawcmp_ex(k, kl, it:key()) <= 0 then
			it:prev()
		end
	elseif memory.rawcmp_ex(k, kl, it:key()) < 0 then
		it:prev()
	end
	if it:valid() then
		if reject_boundary then
			if memory.rawcmp_ex(from_k, from_kl, it:key()) < 0 then
				return it
			end
		elseif memory.rawcmp_ex(from_k, from_kl, it:key()) <= 0 then
			return it
		end
	end
end
-- seek first key which is k, kl <=/< (key) <=/< until_k, until_kl
-- (reject_boundry false/true, respectively)
function mvcc_mt:seek_next(k, kl, until_k, until_kl, reject_boundary)
	local it = self.db:iterator()
	it:seek(k, kl)
	if it:valid() then
		if reject_boundary then
			if memory.rawcmp_ex(until_k, until_kl, it:key()) > 0 then
				return it
			end
		elseif memory.rawcmp_ex(until_k, until_kl, it:key()) >= 0 then
			return it
		end
	end
end	
function mvcc_mt:get(k, ts, txn, opts)
	local v, vl, value_ts = self:rawget(k, #k, ts, txn, opts)
	if v then
		local s = ffi.string(v, vl)
		memory.free(v)
		return s, value_ts
	end
end
--[[
get 
1. 確定した値の時刻以降の値を読みたい場合
transactionが進行中のキーは、そのtransactionからの読み取りである場合を除き、最後に確定した値の時刻以降の値を読むことはできないのでエラー
そのtransactionの読み取りである場合、リトライが発生したと考えられる場合は、最新のキーはそのtransaction自体の書き込みであるので、そのtransaction自身にも見えてはいけない値。よってその１つ前のキーを使う(たぶんtransactionによる新しい値の書き込みは最大１つまでにputInternalで制限されている)
リトライが発生してないと考えられる場合は、その値を返す

2. 確定した値の時刻以前の値を時刻誤差を考慮に入れて(transactionの一部として)読む場合
txnの時刻の最大誤差より最後に確定した値の時刻が前の場合、正しい値が読み出せるか怪しいのでエラーになる
そうでない場合、txnの時刻の最大誤差より前の時刻を持つ中で最新のキーを調べる。そのキーの時刻が、読み出したい時刻よりも後の場合、そのキーはおそらくtxnによって書き込まれた未確定のキーであるためエラー
そうでない場合はその値を返す

3. transactionが指定されておらず、確定した値の時刻以前の値を読みだしたい場合で、時刻誤差を考えても安全に読み出せると考えられる場合
そのまま与えられた時刻を使って、それよりも昔のキーの内最新のものを取得する。

]]
function mvcc_mt:rawget(k, kl, ts, txn, opts)
	local mk, mkl = _M.bytes_codec:encode(k, kl)
	local meta, ml = self.db:rawget(mk, mkl, opts)
	if meta == ffi.NULL then
		return nil
	end
	if ml ~= ffi.sizeof('luact_mvcc_metadata_t') then
		exception.raise('fatal', 'invalid metadata size', ml, ffi.sizeof('luact_mvcc_metadata_t'))
	end
	local ok, v, vl, ts = pcall(self.rawget_internal, self, k, kl, meta, ml, ts, txn, opts)
	memory.free(meta)
	if ok then 
		return v, vl, ts
	else
		error(v)
	end
end
function mvcc_mt:rawget_internal(k, kl, meta, ml, ts, txn, opts)
	local iter, v, vl, value_ts
	-- metadata sanity check
	meta = ffi.cast('luact_mvcc_metadata_t*', meta)
	-- If value is inline, return immediately; txn & timestamp are irrelevant.
	if meta:inline() then
		return meta.value, meta.vlen, ts
	end

	-- First case: Our read timestamp is ahead of the latest write, or the
	-- latest write and current read are within the same transaction.
	local same_txn = meta.txn:valid() and txn and (txn == meta.txn)
	if (ts >= meta.timestamp) or same_txn then
		if meta.txn:valid() and (not (txn and txn:same_origin(meta.txn))) then
			-- if txn already exists, only same txn can read latest value.
			exception.raise('mvcc', 'txn_exists', pstr(k, kl), meta.txn:clone(), txn)
		end
		local latest_key, latest_key_len = _M.bytes_codec:encode(k, kl, meta.timestamp)

		if meta.txn:valid() and (txn.n_retry ~= meta.txn.n_retry) then
			-- same txn but it retrying transaction. so current latest value may be written by previous (failed) txn.
			-- in this case, we seek just before version of latest value to ignore it.
			local key, key_len = _M.bytes_codec:encode(k, kl)
			-- ignore boundary
			iter = self:seek_prev(latest_key, latest_key_len, key, key_len, true) 
		else
			-- latest write and read in the same txn (no retry is possible). then use latest version of value.
			-- dump_db(self.db)
			-- _M.dump_key(latest_key, latest_key_len)
			v, vl = self.db:rawget(latest_key, latest_key_len, opts)
			value_ts = meta.timestamp:clone(true)
			goto RETURN_VALUE
		end
	elseif txn and (ts < txn:max_timestamp()) then
		-- In this branch, the latest timestamp is ahead, and so the read of an
		-- "old" value in a transactional context at time (timestamp, MaxTimestamp]
		-- occurs, leading to a clock uncertainty error if a version exists in
		-- that time interval.
		if txn:max_timestamp() > meta.timestamp then
			-- Second case: Our read timestamp is behind the latest write, but the
			-- latest write could possibly have happened before our read in
			-- absolute time if the writer had a fast clock.
			-- The reader should try again with a later timestamp than the
			-- one given below.
			exception.raise('mvcc', 'txn_ts_uncertainty', pstr(k, kl), meta.timestamp:clone(true), txn:max_timestamp())
		end

		-- We want to know if anything has been written ahead of timestamp, but
		-- before MaxTimestamp.
		local newest_key, newest_key_len = _M.bytes_codec:encode(k, kl, txn:max_timestamp())
		iter = self:seek_prev(newest_key, newest_key_len, _M.bytes_codec:encode(k, kl))
		if iter then
			local newest_ts = _M.bytes_codec:timestamp_of(iter)
			if newest_ts and (newest_ts > ts) then
				-- Third case: Our read timestamp is sufficiently behind the newest
				-- value, but there is another previous write with the same issues
				-- as in the second case, so the reader will have to come again
				-- with a higher read timestamp.
				exception.raise('mvcc', 'txn_ts_uncertainty', pstr(k, kl), newest_ts, ts)
			end
		end
		-- Fourth case: There's no value in our future up to MaxTimestamp, and
		-- those are the only ones that we're not certain about. The correct
		-- key has already been read above, so there's nothing left to do.
	else
		-- Fifth case: We're reading a historic value either outside of
		-- a transaction, or in the absence of future versions that clock
		-- uncertainty would apply to.
		local cur_key, cur_key_len = _M.bytes_codec:encode(k, kl, ts)
		iter = self:seek_prev(cur_key, cur_key_len, _M.bytes_codec:encode(k, kl))
		-- print('iter', iter)
		-- _M.dump_key(cur_key, cur_key_len)
		-- _M.dump_key(iter:key())
	end
	if not iter then
		return nil
	end

	value_ts = _M.bytes_codec:timestamp_of(iter)
	if not value_ts then
		-- print('value_ts nil')
		return nil
	end
	-- allocate own memory
	k, kl = iter:key()
	v, vl = self.db:rawget(k, kl, opts)
::RETURN_VALUE::
	if vl > 0 then
		return v, vl, value_ts
	else
		return nil
	end
end
function mvcc_mt:put(stats, k, v, ts, txn, opts)
	return self:rawput(stats, k, #k, v, #v, ts, txn, opts)
end
function mvcc_mt:rawput(stats, k, kl, v, vl, ts, txn, opts, deleted)
	local mk, mkl = _M.bytes_codec:encode(k, kl)
	local meta, ml = self.db:rawget(mk, mkl, opts)
	if ml > 0 and ml ~= ffi.sizeof('luact_mvcc_metadata_t') then
		exception.raise('fatal', 'invalid metadata size', ml, ffi.sizeof('luact_mvcc_metadata_t'))
	end
	local ok, r = pcall(self.rawput_internal, self, stats, k, kl, v, vl, mk, mkl, meta, ml, ts, txn, opts, deleted)
	memory.free(meta)
	if not ok then
		error(r)
	end
end
--[[
put

1. metadataがすでにある（存在している値）
 a. すでに自分と異なるtxnによってロックされている場合エラー
 b. ロックされていないか、リトライ回数が上のtransactionの中で書こうとしている場合、metadataに自身のtxnとtimestampを書き込む。versioned keyを作ってそこに値を書く。
  また以前のtransactionによって書き込まれたバージョンを削除する（未確定のversioned keyを１つに制限するため）
 c. transactionがない場合、なぞ。(エラーになりそう)
  キーはどのtransactionにもロックされていない。また書き込み側もtransactionを持って書き込んでいない。つまりこの書き込みが成功すれば値がそのまま更新されるべき。
 d. ロックされていない場合で、書き込もうとしている時間がすでに確定したキーの内最新の時刻より前の場合、そのような値を書き込むことはできないのでエラー
 e. それ以外の場合、おそらくロックされているtransactionにおけるより古い書き込みを行おうとしているので、無視する（すでにより新しい値が書かれている）
2. metadataがない
 a. 削除したい場合はすでに削除されているので何もしない
 b. それ以外の場合、metadataに現在のtransactionとtimestampを書き込む。versioned keyを作ってそこに値を書く
]]
local prev_meta_work = memory.alloc_typed('luact_mvcc_metadata_t')
function mvcc_mt:rawput_internal(stats, k, kl, v, vl, mk, mkl, meta, ml, ts, txn, opts, deleted)
	if (not deleted) and (vl <= 0) then
		exception.raise('mvcc', 'empty_value')
	end
	local exists = true
	-- local origAgeSeconds = math.floor((ts:walltime() - meta.timestamp:walltime())/1000)

	if meta == ffi.NULL then
		meta = ffi.new('luact_mvcc_metadata_t')
		exists = false
	else
		meta = ffi.cast('luact_mvcc_metadata_t*', meta)
	end
	ffi.copy(prev_meta_work, meta, ffi.sizeof('luact_mvcc_metadata_t'))
	-- Verify we are not mixing inline and non-inline values.
	-- TODO : support inline read/write?
	local inline = (ts == lamport.ZERO_HLC)
	if inline ~= meta:inline() then
		exception.raise('mvcc', 'key_op', 'mixing inline and non-inline operation')
	end
	if inline then
		-- TODO : also we consider stats update on inline mode
		if deleted then
			self.db:rawdelete(mk, mkl, opts)
		else
			meta:set_current_kv_len(mkl, vl)
			self.db:rawput(mk, mkl, ffi.cast('char *', meta), #meta, opts)
		end
		stats:inline(mkl, prev_meta_work[0], meta, exists)
		return
	end

	-- In case the key metadata exists.
	if exists then
		-- There is an uncommitted write intent and the current Put
		-- operation does not come from the same transaction.
		-- This should not happen since range should check the existing
		-- write intent before executing any Put action at MVCC level.
		if meta.txn:valid() and (not (txn and meta.txn:same_origin(txn))) then
			exception.raise('mvcc', 'txn_exists', pstr(k, kl), meta.txn:clone(), txn)
		end

		-- We can update the current metadata only if both the timestamp
		-- and epoch of the new intent are greater than or equal to
		-- existing. If either of these conditions doesnt hold, its
		-- likely the case that an older RPC is arriving out of order.
		--
		-- Note that if meta.Txn!=nil and txn==nil, a WriteIntentError was
		-- returned above.
		if (ts >= meta.timestamp) and ((not meta.txn:valid()) or (txn.n_retry >= meta.txn.n_retry)) then
			-- If this is an intent and timestamps have changed,
			-- need to remove old version.
			if meta.txn:valid() and (ts ~= meta.timestamp) then
				local prev_key, prev_key_len = _M.bytes_codec:encode(k, kl, meta.timestamp)
				self.db:rawdelete(prev_key, prev_key_len)
			end
			meta:set_txn(txn)
			meta.timestamp = ts
		elseif (meta.timestamp > ts) and (not meta.txn:valid()) then
			-- If we receive a Put request to write before an already-
			-- committed version, send write too old error.
			exception.raise('mvcc', 'write_too_old', pstr(k, kl), meta.timestamp:clone(true), ts)
		else
			-- Otherwise, its an old write to the current transaction. Just ignore.
			return
		end
	else -- In case the key metadata does not exist yet.
		-- If this is a delete, do nothing!
		if deleted then
			return
		end
		-- Create key metadata.
		meta:set_txn(txn)
		meta.timestamp = ts
	end

	-- add deleted flag if specified
	meta:set_deleted(deleted)

	-- TODO : better to use transaction?
	local new_key, new_key_len = _M.bytes_codec:encode(k, kl, ts)
	meta:set_current_kv_len(new_key_len, vl)
	-- _M.dump_key(new_key, new_key_len)
	self.db:rawput(new_key, new_key_len, v, vl)
	-- Write the mvcc metadata now that we have sizes for the latest versioned value.
	self.db:rawput(mk, mkl, ffi.cast("char *", meta), #meta, opts)
	stats:put(mkl, prev_meta_work[0], meta, exists)
end
function mvcc_mt:delete(stats, k, ts, txn, opts)
	return self:rawdelete(stats, k, #k, ts, txn, opts)
end
function mvcc_mt:rawdelete(stats, k, kl, ts, txn, opts)
	return self:rawput(stats, k, kl, "", 0, ts, txn, opts, true)
end
function mvcc_mt:delete_range(stats, s, e, ts, txn, opts)
	return self:rawdelete_range(stats, s, #s, e, #e, ts, txn, opts)
end
function mvcc_mt:delete_filter(k, kl, v, vl, ctx, stats, ts, txn, opts)
	self:rawdelete(stats, k, kl, ts, txn, opts)
	if ctx.count > 0 then
		ctx.count = ctx.count - 1
		return ctx.count <= 0
	else
		ctx.count = ctx.count - 1 -- count delete num as negative value
	end
end
function mvcc_mt:rawdelete_range(stats, s, sl, e, el, n, ts, txn, opts)
	local ctx = { count = n }
	self:rawscan(s, sl, e, el, opts, self.delete_filter, ctx, stats, ts, txn, opts)
	return n - ctx.count
end
function mvcc_mt:merge(stats, k, v, merge_op, ts, txn, opts)
	return self:rawmerge(stats, k, #k, v, #v, merge_op, ts, txn, opts)
end
local pvl_work = memory.alloc_typed('size_t')
function mvcc_mt:rawmerge(stats, k, kl, v, vl, merge_op, ts, txn, opts)
	if not mergers[merge_op] then
		exception.raise('not_found', 'no merger', merge_op)
	end
	local cv, cvl = self:rawget(k, kl, ts, txn, opts)
	local r = {mergers[merge_op](k, kl, cv, cvl, v, vl, pvl_work)}
	if r[1] then
		if pvl_work[0] > 0 then
			self:rawput(stats, k, kl, r[1], pvl_work[0], ts, txn, opts)
		else
			self:rawdelete(stats, k, kl, ts, txn, opts)
		end
	end
	return unpack(r, 2)
end
function mvcc_mt:cas(stats, k, ov, nv, ts, txn, opts)
	return self:rawcas(stats, k, #k, ov, #ov, nv, #nv, ts, txn, opts)
end
function mvcc_mt:rawcas(stats, k, kl, ov, ovl, nv, nvl, ts, txn, opts)
	local op = _M.op_cas(ov, nv, ovl, nvl)
	self:rawmerge(stats, k, kl, ffi.cast('char *', op), #op, 'cas', ts, txn, opts)
	return op:result()
end
function mvcc_mt:resolve_version(stats, k, kl, ts, txn, opts)
	local mk, mkl = _M.bytes_codec:encode(k, kl)
	local v, vl = self.db:rawget(mk, mkl, opts)
	if v ~= ffi.NULL then
		local ok, r = pcall(self.resolve_version_internal, self, stats, k, kl, v, vl, ts, txn, opts)
		memory.free(v)
		if not ok then
			error(r)
		end
	else
		print('meta not exists', _M.inspect_key(mk, mkl))
	end
end
local orig_timestamp_work = ffi.new('pulpo_hlc_t')
function mvcc_mt:resolve_version_internal(stats, k, kl, v, vl, ts, txn, opts)
	if not txn then
		logger.warn('resolve_version', 'no txn specified')
		return
	end
	local mk, mkl = _M.bytes_codec:encode(k, kl)
	if vl ~= ffi.sizeof('luact_mvcc_metadata_t') then
		exception.raise('invalid', 'metadata size', vl, ffi.sizeof('luact_mvcc_metadata_t'))
	end
	local meta = ffi.cast('luact_mvcc_metadata_t*', v)
	ffi.copy(prev_meta_work, meta, ffi.sizeof('luact_mvcc_metadata_t'))
	-- For cases where there's no write intent to resolve, or one exists
	-- which we can't resolve, this is a noop.
	if meta == ffi.NULL or (not (meta.txn:valid() and meta.txn:same_origin(txn))) then
		-- print('matadata problem', meta, meta.txn, txn)
		return
	end
	local origAgeSeconds = math.floor((ts:walltime() - meta.timestamp:walltime())/1000)

	-- If we're committing, or if the commit timestamp of the intent has
	-- been moved forward, and if the proposed epoch matches the existing
	-- epoch: update the meta.Txn. For commit, it's set to nil;
	-- otherwise, we update its value. We may have to update the actual
	-- version value (remove old and create new with proper
	-- timestamp-encoded key) if timestamp changed.
	local commit = (txn.status == txncoord.STATUS_COMMITTED)
	local pushed = (txn.status == txncoord.STATUS_PENDING and meta.txn.timestamp < txn.timestamp)
	-- print('check commit or pushed', commit, pushed, meta.txn.n_retry, txn.n_retry, txn.status)
	if (commit or pushed) and meta.txn.n_retry == txn.n_retry then
		-- print('commit or pushed')
		ffi.copy(orig_timestamp_work, meta.timestamp, ffi.sizeof(meta.timestamp))
		meta.timestamp = txn.timestamp
		if pushed then -- keep intent if we're pushing timestamp
			meta.txn = txn
		else
			meta.txn:invalidate()
		end
		self.db:rawput(mk, mkl, ffi.cast('char *', meta), #meta, opts)		
		-- If timestamp of value changed, need to rewrite versioned value.
		-- TODO(spencer,tobias): think about a new merge operator for
		-- updating key of intent value to new timestamp instead of
		-- read-then-write.
		if orig_timestamp_work ~= txn.timestamp then
			local orig_key, orig_key_len = _M.bytes_codec:encode(k, kl, orig_timestamp_work)
			local new_key, new_key_len = _M.bytes_codec:encode(k, kl, txn.timestamp)
			local v, vl = self.db:rawget(orig_key, orig_key_len, opts)
			if v == ffi.NULL then
				exception.raise('mvcc', 'value_not_found', 'target version', orig_key)
			end
			self.db:rawdelete(orig_key, orig_key_len, opts)
			self.db:rawput(new_key, new_key_len, v, vl, opts)
		end
		stats:committed(mkl, prev_meta_work[0], meta)
		return
	end

	-- This method shouldn't be called with this instance, but there's
	-- nothing to do if the epochs match and the state is still PENDING.
	if txn.status == txncoord.STATUS_PENDING and meta.txn.n_retry == txn.n_retry then
		return
	end

	-- Otherwise, we're deleting the intent. We must find the next
	-- versioned value and reset the metadata's latest timestamp. If
	-- there are no other versioned values, we delete the metadata
	-- key.

	-- First clear the intent value.
	local latest_key, latest_key_len = _M.bytes_codec:encode(k, kl, meta.timestamp)
	self.db:rawdelete(latest_key, latest_key_len, opts)

	-- Compute the next possible mvcc value for this key.
	local limit_key, limit_key_len = _M.bytes_codec:encode(k, kl)
	-- print('find possible key')
	-- _M.dump_key(latest_key, latest_key_len)
	-- _M.dump_key(limit_key, limit_key_len)
	-- dump_db(self.db)
	-- Compute the last possible mvcc value for this key. (ignore boundary)
	local iter = self:seek_prev(latest_key, latest_key_len, limit_key, limit_key_len, true)
	if not iter then
	-- print('no possible key: delete meta')
		self.db:rawdelete(mk, mkl, opts)
	else
	-- print('possible key exists', _M.dump_key(iter:key()))
		local prev_k, prev_kl = iter:key()
		local prev_v, prev_vl = iter:val()
		local _k, _kl, timestamp = _M.bytes_codec:decode(prev_k, prev_kl)
		if not timestamp then
			exception.raise('mvcc', 'invalid_key', 'expected an MVCC value key', pstr(iter:key()))
		end
		-- Get the bytes for the next version so we have size for stat counts.
		--[[
		local v, vl = self.db:rawget(curr_k, curr_kl, opts)
		if v == ffi.NULL then
			exception.raise('mvcc', 'value_not_found', 'previous version for key', pstr(k, kl))
		end
		]]
		-- Update the keyMetadata with the next version.
		meta.timestamp = timestamp[0]
		meta.txn:invalidate()
		meta:set_current_kv_len(prev_kl, prev_vl)
		-- meta:set_deleted()
		self.db:rawput(mk, mkl, ffi.cast('char *', meta), #meta, opts)
		local restoredAgeSeconds = math.floor((ts:walltime() - timestamp:walltime())/1000)

		-- Update stat counters with older version.
		-- ms.updateStatsOnAbort(key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize, meta, newMeta, origAgeSeconds, restoredAgeSeconds)
	end
	-- dump_db(self.db)
	stats:aborted(mkl, prev_meta_work[0], meta, iter)
end
function mvcc_mt:resolve_version_filter(k, kl, v, vl, ctx, stats, ts, txn, opts)
	self:resolve_version_internal(stats, k, kl, v, vl, ts, txn, opts)
	if ctx.count > 0 then
		ctx.count = ctx.count - 1
		return ctx.count <= 0
	else
		ctx.count = ctx.count - 1 -- count delete num as negative value
	end	
end
function mvcc_mt:resolve_versions_in_range(stats, s, sl, e, el, n, ts, txn, opts)
	local ctx = { count = n }
	self:rawscan(s, sl, e, el, opts, self.resolve_version_filter, ctx, stats, ts, txn, opts)
	return n - ctx.count
end
local split_key_work = memory.alloc_typed('char', 256)
local split_key_work_size = 256
function mvcc_mt:find_split_key(st, s, sl, e, el, checker)
	--print('find_split_key ----------------------------')
	-- dump_db(self.db)
	local it = self.db:iterator(opts)
	local desired_size = math.floor((tonumber(st.bytes_val + st.bytes_key)) / 2)
	local current_bytes = 0
	local k, kl, v, vl, best_k, best_kl
	local best_diff = 0xFFFFFFFFULL
	it:seek(_M.bytes_codec:encode(s, sl)) --> seek to the smallest of bigger key
	-- print('find_split_key: desired_size=', desired_size)
	while it:valid() do
		k, kl = it:key()
		v, vl = it:val()
		local dk, dkl = _M.bytes_codec:decode(k, kl)
		if (not checker) or checker(dk, dkl) then
			local diff = math.abs(tonumber(desired_size) - tonumber(current_bytes))
			if diff < best_diff then
			-- print('diff', best_diff, diff, pstr(dk, dkl), desired_size, current_bytes)
				best_diff = diff
				if split_key_work_size < dkl then
					while split_key_work_size < dkl do
						split_key_work_size = split_key_work_size * 2
					end
					local tmp = memory.realloc_typed('char', split_key_work, split_key_work_size)
					if tmp == ffi.NULL then
						exception.raise('fatal', 'malloc', split_key_work_size)
					end
					split_key_work = tmp
				end
				ffi.copy(split_key_work, dk, dkl)
				best_k, best_kl = split_key_work, dkl
			elseif best_k then
				-- print('exit', pstr(dk, dkl))
				break
			end
		end
		current_bytes = current_bytes + kl + vl
		it:next()
	end
	if not best_k then
		logger.report('range', 'cannot split', pstr(s, sl), pstr(e, el))
		-- TODO : how can we treat this?
	else
		return best_k, best_kl
	end
end
function mvcc_mt:compute_stats_filter(k, kl, v, vl, ctx, st, wt)
	local rk, rkl, ts = _M.bytes_codec:decode(k, kl)
	if ts then
		-- versioned key
	else
		-- meta key
		st.n_key = st.n_key + 1
	end
	st.n_val = st.n_val + 1
	-- print('compute_stats_filter', st.bytes_key, st.bytes_key + kl, st.bytes_val, st.bytes_val + vl)
	st.bytes_key = st.bytes_key + kl
	st.bytes_val = st.bytes_val + vl
end
function mvcc_mt:compute_stats(s, sl, e, el, ts, opts)
	local ctx = {}
	local st = ffi.new('luact_mvcc_stats_t')
	st:init()
	self:rawscan_all(s, sl, e, el, opts, self.compute_stats_filter, ctx, st, ts:walltime())	
	return st
end
function mvcc_mt:gc(stats, keys, opts)
	local it = self.db:iterator(opts)
	for i=1,#keys do
		local gc_key, gc_key_len = ffi.cast('const char *', keys[i]), #keys[i]
		local k, kl, limit_ts = _M.bytes_codec:decode(gc_key, gc_key_len)
		if not limit_ts then
			exception.raise('invalid', 'no timestamp specified', _M.inspect_key(gc_key, gc_key_len))
		end
		local mk, mkl = _M.bytes_codec:encode(k, kl)
		it:seek(mk, mkl)
		if not it:valid() then
			exception.raise('not_found', 'gc meta key', _M.inspect_key(mk, mkl))
		end
		local v, vl = it:val()
		local meta = ffi.cast('luact_mvcc_metadata_t*', v)
		-- First, check whether all values of the key are being deleted.
		if limit_ts >= meta.timestamp then
			local nk, nkl = _M.bytes_codec:encode(_M.bytes_codec:next_of(k, kl))
			local it_latest_val = self:seek_prev(nk, nkl, mk, mkl)
			if not it_latest_val then
				exception.raise('not_found', 'last versioned key', _M.inspect_key(lk, lkl))
			end
			local nv, nvl = it_latest_val:val()
			if nvl > 0 then
				exception.raise('mvcc', 'gc_non_deleted_value', _M.inspect_key(mk, mkl))
			end
			if meta.txn:valid() then
				exception.raise('mvcc', 'gc_uncommitted_value', _M.inspect_key(mk, mkl))
			end
			self.db:rawdelete(mk, mkl)
			stats:gc(mkl, vl)
		end
		it:next() -- seek to oldest versioned key. because it have own snapshot, even if meta key has deleted, it works.

		-- Now, iterate through all values, GC'ing ones which have expired.
		local _k, _kl, ts
		while it:valid() do
			k, kl = it:key()
			_k, _kl, ts = _M.bytes_codec:decode(k, kl)
			if not ts then -- reach to next metakey. finished
				break
			-- both are pointer. so make ref to call operator override correctly
			elseif ts[0] <= limit_ts[0] then
				v, vl = it:val()
				-- print('rawdelete', _M.inspect_key(k, kl))
				self.db:rawdelete(k, kl)
				stats:gc(kl, vl)
			end
			it:next()
		end
	end
end



-- module funcitons
function _M.new_mt()
	local mt = util.copy_table(mvcc_mt)
	mt.__index = mt
	return mt
end
_M.bytes_codec = memory.alloc_typed('luact_mvcc_bytes_codec_t')
_M.bytes_codec:init(32)
function _M.make_key(k, kl, ts)
	return ffi.string(_M.bytes_codec:encode(k, kl, ts))
end
function _M.upper_bound_of_prefix(k, kl)
	return _M.bytes_codec:upper_bound_of_prefix(k, kl or #k)
end
function _M.register_merger(name, callable)
	mergers[name] = callable
end
function _M.inspect_key(k, kl)
	local dk, dkl, ts = _M.bytes_codec:decode(k, kl)
	local src = {'key:('..tostring(tonumber(kl))..')'..tostring(k)}
	for i=0,dkl-1 do
		table.insert(src, (':%02x'):format(ffi.cast('const unsigned char *', dk)[i]))
	end
	if ts then
		table.insert(src, (' @ ')..tostring(ts))
	end
	return table.concat(src)
end
-- cas merger
_M.register_merger('cas', merger_cas_mt.process)


-- debug
function _M.dump_key(k, kl)
	if type(k) == 'string' then
		kl = #k
		k = ffi.cast('const char *', k)
	end
if false then
	io.write('key:')
	for i=0,tonumber(kl)-1 do
		io.write((':%02x'):format(ffi.cast('const unsigned char *', k)[i]))
	end
else
	io.write(_M.inspect_key(k, kl))
end
	io.write('\n')
end

return _M

