--[[
	manages multiple value for single key by using versioned key( = keyname + lamport style logical timestamp )
	most part of initial code is from cockroachDB's mvcc.go. thanks to cockroach authors for well documented codes :D
]]
local ffi = require 'ffiex.init'

local txncoord = require 'luact.storage.txncoord'
local buffer = require 'luact.util.buffer'
local lamport = require 'pulpo.lamport'
local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'

local _M = {}

-- cdef
ffi.cdef [[
typedef enum luact_mvcc_key_type {
	MVCC_KEY_VALUE,
	MVCC_KEY_VERSIONED,
} luact_mvcc_key_type_t;

typedef struct luact_mvcc_key_codec {
	uint16_t size, idx;
	uint32_t *lengths;
	char **buffers;
} luact_mvcc_key_codec_t;

typedef struct luact_mvcc_metadata {
	pulpo_hlc_t timestamp, max_timestamp;
	luact_dht_txn_t txn;
	uint8_t delete_flag, reserved[3];
} luact_mvcc_metadata_t;
]]
local MVCC_KEY_VALUE = ffi.cast('luact_mvcc_key_type_t', 'MVCC_KEY_VALUE')
local MVCC_KEY_VERSIONED = ffi.cast('luact_mvcc_key_type_t', 'MVCC_KEY_VERSIONED')



-- exception
exception.define('mvcc')



-- local functions
local function pstr(k, kl)
	return ('%q'):format(ffi.string(k, kl))
end



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
ffi.metatype('luact_mvcc_metadata_t', mvcc_meta_mt)



-- mvcc key
local mvcc_key_codec_mt = {}
mvcc_key_codec_mt.__index = mvcc_key_codec_mt
function mvcc_key_codec_mt:init(size)
	self.size = size
	self.idx = 0
	self.lengths = memory.alloc_fill_typed('uint32_t', size)
	self.buffers = memory.alloc_fill_typed('char *', size)
	for i=0, self.size - 1 do
		self.lengths[i] = 256
		self.buffers[i] = memory.alloc(self.lengths[i])
	end
end
function mvcc_key_codec_mt:reserve(k, kl, ts)
	-- +1 for using in self:next_of
	local reqlen = (util.encode_binary_length(kl + (ts and ffi.sizeof('pulpo_hlc_t') or 0)) + 1)
	local len = self.lengths[self.idx]
	if len >= reqlen then 
		self.idx = (self.idx + 1) % self.size
		return self.buffers[self.idx], len 
	end
	while len < reqlen do
		len = len * 2
	end
	local tmp = memory.realloc(self.buffers[self.idx], len)
	if tmp ~= ffi.NULL then
		self.buffers[self.idx] = tmp
		self.lengths[self.idx] = len
		self.idx = (self.idx + 1) % self.size
		return tmp, len
	else
		exception.raise('fatal', 'malloc', len)
	end
end
function mvcc_key_codec_mt:encode(k, kl, ts)
	local _, olen, ofs
	local p, len = self:reserve(k, kl, ts)
	_, ofs = util.encode_binary(k, kl, p, len)
	if ts then
		_, olen = util.encode_binary(ts.p, ffi.sizeof(ts), p + ofs, len - ofs)
		ofs = ofs + olen
	end
	return p, ofs
end
function mvcc_key_codec_mt:decode(ek, ekl)
	local p, len = self:reserve(ek, ekl, true)
	local k, kl, n_read = util.decode_binary(ek, ekl, p, len)
	if n_read >= ekl then
		return k, kl
	else
		local ts, tsl = util.decode_binary(ek + n_read, ekl - n_read, p + kl, len - kl)
		return k, kl, ffi.cast('pulpo_hlc_t *', ts)
	end
end
function mvcc_key_codec_mt:available(p, pl)
	for i=0, self.size - 1 do
		if self.buffers[i] == p then
			return (self.lengths[i] - pl) > 0
		end
	end
end
function mvcc_key_codec_mt:next_of(k, kl)
	assert(self:available(k, kl))
	k[kl] = 0
	return k, kl + 1
end
function mvcc_key_codec_mt:timestamp_of(iter)
	local k, kl, ts = self:decode(iter:key())
	return ts
end
ffi.metatype('luact_mvcc_key_codec_t', mvcc_key_codec_mt)



-- mvcc ctype
local mvcc_mt = {}
function mvcc_mt:init(db)
	self.db = db
end
function mvcc_mt:close()
	self.db:fin()
end
function mvcc_mt:column_family(name, opts)
	exception.raise('mvcc', 'not_support', 'please implement it properly for each kind of mvcc')
end
function mvcc_mt:scan(s, e, cb, ...)
	return self:rawscan(s, #s, e, #e, cb, ...)
end
function mvcc_mt:rawscan(s, sl, e, el, cb, ...)
	local keys = {}
	local it = self.db:iterator()
	it:seek(s, sl) --> seek to the smallest of bigger key
	while it:valid() do
		local k, kl = it:key()	
		if memory.rawcmp_ex2(e, el, k, kl) < 0 then
			break
		end
		local v, vl = it:val()
		if cb(self, k, kl, v, vl, ...) == false then
			break
		end
		it:next()
	end
end
function mvcc_mt:scan_meta(s, sl, e, el, cb, ...)
	local keys = {}
	local it = self.db:iterator()
	it:seek(s, sl) --> seek to the smallest of bigger key
	while it:valid() do
		local k, kl, ts = _M.key_codec:decode(iter:key())
		if ts then
			exception.raise('mvcc', 'invalid_key', 'start key should not be versioned key', pstr(s, sl))
		end
		if memory.rawcmp_ex2(e, el, k, kl) < 0 then
			break
		end
		local v, vl = it:val()
		if cb(self, k, kl, v, vl, ...) == false then
			break
		end
		k, kl = _M.key_codec:next_of(k, kl)
		it:seek(k, kl) -- effectively skip all versioned key
	end
end
function mvcc_mt:seek_prev(k, kl)
	local it = self.db:iterator()
	it:seek(k, kl)
	if not it:valid() then
		it:last()
	end
	if it:valid() then
		it:prev()
		return it
	end
end
function mvcc_mt:seek_next(k, kl, until_k, until_kl)
	local it = self.db:iterator()
	it:seek(k, kl)
	if it:valid() then
		if memory.rawcmp_ex(until_k, until_kl, it:key()) >= 0 then
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
function mvcc_mt:rawget(k, kl, ts, txn, opts)
	if kl <= 0 then
		exception.raise('mvcc', 'empty_key')
	end
	local meta, ml = self.db:rawget(k, kl, opts)
	local iter
	if meta == ffi.NULL then
		return nil, 0
	end
	if ml ~= ffi.sizeof('luact_mvcc_metadata_t') then
		exception.raise('fatal', 'invalid metadata size', ml, ffi.sizeof('luact_mvcc_metadata_t'))
	end
	meta = ffi.cast('luact_mvcc_metadata_t*', meta)
	if meta:deleted() then
		return nil, 0
	end
	-- If value is inline, return immediately; txn & timestamp are irrelevant.
	if meta:inline() then
		return meta.value, meta.vlen
	end

	-- First case: Our read timestamp is ahead of the latest write, or the
	-- latest write and current read are within the same transaction.
	local same_txn = meta.txn:valid() and txn and (txn == meta.txn)
	if (ts >= meta.timestamp) or same_txn then
		if meta.txn:valid() and (not (txn and txn:same_origin(meta.txn))) then
			-- Trying to read the last value, but it's another transaction's
			-- intent; the reader will have to act on this.
			exception.raise('mvcc', 'invalid_txn', 'locked by another txn', pstr(k, kl), meta.txn, txn)
		end
		local latest_key, latest_key_len = _M.key_codec:encode(k, kl, meta.timestamp)

		-- Check for case where we're reading our own txn's intent
		-- but it's got a different epoch. This can happen if the
		-- txn was restarted and an earlier iteration wrote the value
		-- we're now reading. In this case, we skip the intent.
		if meta.txn:valid() and (txn.n_retry ~= meta.txn.n_retry) then
			iter = self:seek_prev(latest_key, latest_key_len)
		else
			return self.db:rawget(latest_key, latest_key_len)
		end
	elseif txn and (ts < txn:max_timestamp()) then
		-- In this branch, the latest timestamp is ahead, and so the read of an
		-- "old" value in a transactional context at time (timestamp, MaxTimestamp]
		-- occurs, leading to a clock uncertainty error if a version exists in
		-- that time interval.
		if txn:max_timestamp() < meta.timestamp then
			-- Second case: Our read timestamp is behind the latest write, but the
			-- latest write could possibly have happened before our read in
			-- absolute time if the writer had a fast clock.
			-- The reader should try again with a later timestamp than the
			-- one given below.
			exception.raise('mvcc', 'txn_ts_uncertainty', pstr(k, kl), meta.timestamp, txn:max_timestamp())
		end

		-- We want to know if anything has been written ahead of timestamp, but
		-- before MaxTimestamp.
		local newest_key, newest_key_len = _M.key_codec:encode(k, kl, txn:max_timestamp())
		iter = self:seek_prev(newest_key, newest_key_len)
		if iter then
			local newest_ts = _M.key_codec:timestamp_of(iter)
			if newest_ts and newest_ts > ts then
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
		local cur_key, cur_key_len = _M.key_codec:encode(k, kl, ts)
		iter = self:seek_prev(cur_key, cur_key_len)
	end
	if not iter then
		return nil, 0
	end

	local value_ts = _M.key_codec:timestamp_of(iter)
	local v, vl = iter:val()
	return v, vl, value_ts
end
function mvcc_mt:put(k, v, ts, txn, opts)
	return self:rawput(k, #k, v, #v, ts, opts)
end
function mvcc_mt:rawput(k, kl, v, vl, ts, txn, opts, deleted)
	if kl <= 0 then
		exception.raise('mvcc', 'empty_key')
	end
	local meta, ml = self.db:rawget(k, kl, opts)
	if ml > 0 and ml ~= ffi.sizeof('luact_mvcc_metadata_t') then
		exception.raise('fatal', 'invalid metadata size', ml, ffi.sizeof('luact_mvcc_metadata_t'))
	end
	local exists = true
	-- local origAgeSeconds = math.floor((ts:walltime() - meta.timestamp:walltime())/1000)

	if meta == ffi.NULL then
		meta = ffi.new('luact_mvcc_metadata_t')
		exists = false
	end
	-- Verify we are not mixing inline and non-inline values.
	local inline = (ts == lamport.ZERO_HLC)
	if inline ~= meta:inline() then
		exception.raise('mvcc', 'key_op', 'mixing inline and non-inline operation')
	end
	if inline then
		if deleted then
			self.db:rawdelete(k, kl, opts)
		else
			meta:set_inline_value(v, vl)
			self.db:rawput(k, kl, meta, #meta, opts)
		end
		return
	end

	-- In case the key metadata exists.
	if exists then
		-- There is an uncommitted write intent and the current Put
		-- operation does not come from the same transaction.
		-- This should not happen since range should check the existing
		-- write intent before executing any Put action at MVCC level.
		if meta.txn:valid() and (not (txn and meta.txn:same_origin(txn))) then
			exception.raise('mvcc', 'invalid_txn', 'locked by another txn', pstr(k, kl), meta.txn, txn)
		end

		-- We can update the current metadata only if both the timestamp
		-- and epoch of the new intent are greater than or equal to
		-- existing. If either of these conditions doesnt hold, its
		-- likely the case that an older RPC is arriving out of order.
		--
		-- Note that if meta.Txn!=nil and txn==nil, a WriteIntentError was
		-- returned above.
		if (ts >= meta.timestamp) and (meta.txn:valid() or (txn.n_retry >= meta.txn.n_retry)) then
			-- If this is an intent and timestamps have changed,
			-- need to remove old version.
			if meta.txn:valid() and (ts ~= meta.timestamp) then
				local prev_key, prev_key_len = _M.key_codec:encode(k, kl, meta.timestamp)
				self.db:rawdelete(prev_key, prev_key_len)
			end
			meta.txn = txn
			meta.timestamp = ts
		elseif (meta.timestamp > ts) and meta.txn:valid() then
			-- If we receive a Put request to write before an already-
			-- committed version, send write tool old error.
			exception.raise('mvcc', 'txn_ts_uncertainty', pstr(k, kl), meta.timestamp, txn:max_timestamp())
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
		if txn then
			meta.txn = txn
		else
			meta.txn:invalidate()
		end
		meta.timestamp = ts
	end

	-- add deleted flag if specified
	meta:set_deleted(deleted)

	-- TODO : better to use transaction?
	local new_key, new_key_len = _M.key_codec:encode(k, kl, ts)
	self.db:rawput(new_key, new_key_len, v, vl)	
	-- Write the mvcc metadata now that we have sizes for the latest versioned value.
	self.db:rawput(k, kl, meta, #meta, opts)
end
function mvcc_mt:delete(k, ts, opts)
	return self:rawdelete(k, #k, ts, opts)
end
function mvcc_mt:rawdelete(k, kl, ts, txn, opts)
	return self:rawput(k, kl, "", 0, txn, opts, true)
end
function mvcc_mt:merge(k, v, ts, opts)
	return self:rawmerge(k, #k, v, #v, ts, opts)
end
function mvcc_mt:rawmerge(k, kl, v, vl, ts, txn, opts)
	exception.raise('invalid', 'now we have not finished defining merge semantics yet!!')
end
function mvcc_mt:new_txn(ts)
	return txncoord.new_txn(ts)
end	
function mvcc_mt:resolve_txn(k, kl, v, vl, txn, opts)
	if kl <= 0 then
		exception.raise('mvcc', 'empty_key')
	end
	if not txn then
		logger.warn('resolve_txn', 'no txn specified')
		return
	end
	local meta, ml = v, vl
	if ml ~= ffi.sizeof('luact_mvcc_metadata_t') then
		exception.raise('fatal', 'invalid metadata size', ml, ffi.sizeof('luact_mvcc_metadata_t'))
	end

	-- For cases where there's no write intent to resolve, or one exists
	-- which we can't resolve, this is a noop.
	if meta == ffi.NULL or (not (meta.txn:valid() and meta.txn:same_origin(txn))) then
		return nil
	end
	local origAgeSeconds = math.floor((ts:walltime() - meta.timestamp:walltime())/1000)

	-- If we're committing, or if the commit timestamp of the intent has
	-- been moved forward, and if the proposed epoch matches the existing
	-- epoch: update the meta.Txn. For commit, it's set to nil;
	-- otherwise, we update its value. We may have to update the actual
	-- version value (remove old and create new with proper
	-- timestamp-encoded key) if timestamp changed.
	local commit = (txn.status == txncoord.TXN_STATUS_COMMITTED)
	local pushed = (txn.status == txncoord.TXN_STATUS_PENDING and meta.txn.timestamp < txn.timestamp)
	if (commit or pushed) and meta.txn.n_retry == txn.n_retry then
		local orig_timestamp = meta.timestamp
		if pushed then -- keep intent if we're pushing timestamp
			meta.txn = txn
		else
			meta.txn:invalidate()
		end
		self.db:rawput(k, kl, meta, #meta, opts)
		
		-- If timestamp of value changed, need to rewrite versioned value.
		-- TODO(spencer,tobias): think about a new merge operator for
		-- updating key of intent value to new timestamp instead of
		-- read-then-write.
		if orig_timestamp ~= txn.timestamp then
			local orig_key, orig_key_len = _M.key_codec:encode(k, kl, orig_timestamp)
			local new_key, new_key_len = _M.key_codec:encode(k, kl, txn.timestamp)
			local v, vl = self.db:rawget(orig_key, orig_key_len, opts)
			if v == ffi.NULL then
				exception.raise('mvcc', 'value_not_found', 'target version', orig_key)
			end
			self.db:rawdelete(orig_key, orig_key_len, opts)
			self.db:rawput(new_key, new_key_len, v, vl, opts)
		end
		return
	end

	-- This method shouldn't be called with this instance, but there's
	-- nothing to do if the epochs match and the state is still PENDING.
	if txn.status == txncoord.TXN_STATUS_PENDING and meta.txn.n_retry == txn.n_retry then
		return
	end

	-- Otherwise, we're deleting the intent. We must find the next
	-- versioned value and reset the metadata's latest timestamp. If
	-- there are no other versioned values, we delete the metadata
	-- key.

	-- First clear the intent value.
	local latest_key, latest_key_len = _M.key_codec:encode(k, kl, meta.timestamp)
	self.db:rawdelete(latest_key, latest_key_len, opts)

	-- Compute the next possible mvcc value for this key.
	local next_key, next_key_len = mvcc_key_mt.next_of(latest_key, latest_key_len)
	local end_scan_key, end_scan_key_len = mvcc_key_mt.next_of(k, kl)
	-- Compute the last possible mvcc value for this key.
	local iter = self:seek_next(next_key, next_key_len, end_scan_key, end_scan_key_len)
	if not iter then
		self.db:rawdelete(k, kl, opts)
	else
		local k, kl = iter:key()
		local key = ffi.cast('luact_mvcc_key_t*', k)
		if not key:versioned(kl) then
			exception.raise('mvcc', 'invalid_key', 'expected an MVCC value key', ('%q'):format(ffi.string(k, kl)))
		end
		-- Get the bytes for the next version so we have size for stat counts.
		local v, vl = self.db:rawget(k, kl, opts)
		if v == ffi.NULL then
			exception.raise('mvcc', 'value_not_found', 'previous version for key', ('%q'):format(ffi.string(k, kl)))
		end
		-- Update the keyMetadata with the next version.
		meta.timestamp = key:timestamp()
		-- meta:set_deleted()
		self.db:rawput(k, kl, meta, #meta, opts)
		local restoredAgeSeconds = math.floor((timestamp:wallTime() - key:timestamp():WallTime())/1000)

		-- Update stat counters with older version.
		-- ms.updateStatsOnAbort(key, origMetaKeySize, origMetaValSize, metaKeySize, metaValSize, meta, newMeta, origAgeSeconds, restoredAgeSeconds)
	end
end
function mvcc_mt:end_txn(s, sl, e, el, txn, opts)
	return self:scan_meta(s, sl, e, el, self.resolve_txn, txn, opts)
end

-- module funcitons
function _M.new_mt()
	local mt = util.copy_table(mvcc_mt)
	mt.__index = mt
	return mt
end
_M.key_codec = memory.alloc_typed('luact_mvcc_key_codec_t')
_M.key_codec:init(16)
function _M.make_key(k, kl, ts)
	return ffi.string(_M.key_codec:encode(k, kl, ts))
end
function _M.dump_key(k, kl)
	if type(k) == 'string' then
		kl = #k
		k = ffi.cast('const char *', k)
	end
	local dk, dkl, ts = _M.key_codec:decode(k, kl)
	io.write('key:', tostring(k))
	for i=0,dkl-1 do
		io.write((':%02x'):format(ffi.cast('const unsigned char *', dk)[i]))
	end
	if ts then
		io.write(' @ ')
		io.write(tostring(ts))
	end
	io.write('\n')
end

return _M

