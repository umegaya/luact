local ffi = require 'ffiex.init'
local util = require 'pulpo.util'
local memory = require 'pulpo.memory'

local _M = {}

ffi.cdef [[
struct luact_dht_cmd_get {
	uint8_t kind, padd;
	uint16_t kl;
	char k[0];
} luact_dht_cmd_get_t;

struct luact_dht_cmd_put {
	uint8_t kind, padd;
	uint16_t kl;
	uint32_t vl;
	char kv[0];	
} luact_dht_cmd_put_t;

struct luact_dht_cmd_cas {
	uint8_t kind, padd;
	uint16_t kl;
	uint32_t ol;
	uint32_t nl;
	char kon[0];		
} luact_dht_cmd_cas_t;

struct luact_dht_cmd_merge {
	uint8_t kind, padd;
	uint16_t kl;
	uint32_t vl;
	char kv[0];		
} luact_dht_cmd_cas_t;

struct luact_dht_cmd_watch {
	uint8_t kind, padd;
	uint16_t kl;
	luact_uuid_t watcher;
	uint16_t name_len;
	uint16_t arg_len;
	char p[0];
} luact_dht_cmd_watch_t;

struct luact_dht_cmd_split {
	uint8_t kind, padd;
	uint16_t kl;
	char k[0];
} luact_dht_cmd_split_t;
]]

-- get command
local get_mt = {}
get_mt.__index = get_mt
function get_mt.size(kl)
	return ffi.sizeof('luact_dht_cmd_get_t') + kl
end
function get_mt.new(kind, k, kl)
	local p = ffi.cast('luact_dht_cmd_get_t*', memory.alloc(get_mt.size(kl)))
	p.kind = kind
	p.kl = kl
	ffi.copy(p:key(), k, kl)
end
_M.get = get_mt.new
function get_mt:key()
	return self.k
end
function get_mt:apply_to(storage, range)
	return range:exec_get(storage, self:key(), self.kl)
end
ffi.metatype('luact_dht_cmd_get_t', get_mt)


-- put command
local put_mt = {}
put_mt.__index = put_mt
function put_mt.size(kl, vl)
	return ffi.sizeof('luact_dht_cmd_put_t') + kl + vl
end
function put_mt.new(kind, k, kl, v, vl)
	local p = ffi.cast('luact_dht_cmd_put_t*', memory.alloc(put_mt.size(kl, vl)))
	p.kind = kind
	p.kl = kl
	p.vl = vl
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:val(), v, vl)
end
_M.put = put_mt.new
function put_mt:key()
	return self.kv
end
function put_mt:val()
	return self.kv + self.kl
end
function put_mt:apply_to(storage, range)
	return range:exec_put(storage, self:key(), self.kl, self:val(), self.vl)
end
ffi.metatype('luact_dht_cmd_put_t', put_mt)


-- cas command
local cas_mt = {}
cas_mt.__index = cas_mt
function cas_mt.size(kl, ol, nl)
	return ffi.sizeof('luact_dht_cmd_cas_t') + kl + ol + nl
end
function cas_mt.new(kind, k, kl, o, ol, n, nl)
	local p = ffi.cast('luact_dht_cmd_cas_t*', memory.alloc(cas_mt.size(kl)))
	p.kind = kind
	p.kl = kl
	p.ol = ol
	p.nl = nl
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:oldval(), o, ol)
	ffi.copy(p:newval(), n, nl)
end
_M.cas = cas_mt.new
function cas_mt:key()
	return self.kv
end
function cas_mt:oldval()
	return self.kv + self.kl
end
function cas_mt:newval()
	return self.kv + self.kl + self.ol
end
function cas_mt:apply_to(storage, range)
	return range:exec_cas(storage, self:key(), self.kl, self:oldval(), self.ol, self:newval(), self.nl)
end
ffi.metatype('luact_dht_cmd_cas_t', cas_mt)


-- merge command
local merge_mt = {}
merge_mt.__index = merge_mt
function merge_mt.size(kl, vl)
	return ffi.sizeof('luact_dht_cmd_merge_t') + kl + vl
end
function merge_mt.new(kind, k, kl, v, vl)
	local p = ffi.cast('luact_dht_cmd_merge_t*', memory.alloc(get_mt.size(kl, vl)))
	p.kind = kind
	p.kl = kl
	p.vl = vl
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:val(), v, vl)
end
_M.merge = merge_mt.new
function merge_mt:key()
	return self.kv
end
function merge_mt:val()
	return self.kv + self.kl
end
function merge_mt:apply_to(storage, range)
	return range:exec_merge(storage, self:key(), self.kl, self:val(), self.vl)
end
ffi.metatype('luact_dht_cmd_merge_t', merge_mt)


-- watch command
local watch_mt = {}
watch_mt.__index = watch_mt
function watch_mt.size(kl)
	return ffi.sizeof('luact_dht_cmd_watch_t') + kl
end
function watch_mt.new(kind, k, kl, watcher, method, arg, arglen)
	local p = ffi.cast('luact_dht_cmd_watch_t*', memory.alloc(watch_mt.size(kl)))
	p.kind = kind
	p.kl = kl
	p.name_len = #method
	p.arg_len = arglen or 0
	ffi.copy(p:key(), k, kl)
	ffi.copy(p:method(), method)
	if arg then
		ffi.copy(p:arg(), arg, arglen)
	end
end
_M.watch = watch_mt.new
function watch_mt:key()
	return self.p
end
function watch_mt:method()
	return self.p + self.kl
end
function watch_mt:arg()
	return self.p + self.kl + self.name_len
end
function watch_mt:apply_to(storage, range)
	if self.arg_len > 0 then
		return range:exec_watch(storage, self:key(), self.kl, ffi.string(self:method(), self.name_len), self:arg(), self.arg_len)
	else
		return range:exec_watch(storage, self:key(), self.kl, ffi.string(self:method(), self.name_len))
	end
end
ffi.metatype('luact_dht_cmd_watch_t', watch_mt)


-- split command
local split_mt = {}
split_mt.__index = split_mt
function split_mt.size(kl)
	return ffi.sizeof('luact_dht_cmd_get_t') + kl
end
function split_mt.new(kind, k, kl)
	local p = ffi.cast('luact_dht_cmd_get_t*', memory.alloc(get_mt.size(kl)))
	p.kind = kind
	p.kl = kl
	ffi.copy(p:key(), k, kl)
end
_M.split = split_mt.new
function split_mt:key()
	return self.k
end
function split_mt:apply_to(storage, range)
	return range:exec_split(storage, self:key(), self.kl)
end
ffi.metatype('luact_dht_cmd_split_t', split_mt)

return _M
