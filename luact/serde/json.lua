-- TODO : treat correctly the case client machine is not little endian.
local ffi = require 'ffiex.init'
local reflect = require 'reflect'
local common = require 'luact.serde.common'
local writer = require 'luact.writer'
local memory = require 'pulpo.memory'
local poller = require 'pulpo.poller'
local thread = require 'pulpo.thread'
local loader = require 'pulpo.loader'
local exception = require 'pulpo.exception'

local WRITER_RAW = writer.WRITER_RAW

local ffi_state, yajl = loader.load('json.lua', {
	"yajl_option", "yajl_gen_option", "yajl_callbacks", "yajl_print_t", "yajl_status",

	"yajl_alloc", "yajl_free", "yajl_config", 
	"yajl_parse", "yajl_complete_parse", "yajl_get_error", 

    "yajl_gen_alloc",  "yajl_gen_free", "yajl_gen_config", 
    "yajl_gen_integer", 
    "yajl_gen_double",
    "yajl_gen_number",
    "yajl_gen_string",
    "yajl_gen_null",
    "yajl_gen_bool",
    "yajl_gen_map_open",
    "yajl_gen_map_close",
    "yajl_gen_array_open",
    "yajl_gen_array_close",
}, {}, "yajl", [[
	#include <yajl/yajl_parse.h>
	#include <yajl/yajl_gen.h>
]])
ffi.cdef[[
	typedef struct luact_json_serde {
		int dummy;
	} luact_json_serde_t;
	typedef struct luact_json_parse_context {
		int id;
		luact_rbuf_t *rb;
		short cs_size, cs_depth;
		struct luact_json_parse_context_stack { 
			int parse_type, last_kl;
			const char *last_k;
		} *cs;
	} luact_json_parse_context_t;
]]

local yajl_status_ok = ffi.cast('yajl_status', 'yajl_status_ok')
local yajl_status_client_canceled = ffi.cast('yajl_status', 'yajl_status_client_canceled')
local yajl_status_error = ffi.cast('yajl_status', 'yajl_status_error')



-- exception
exception.define('json')


-- msgpack serde
local serde_mt = {}
serde_mt.__index = serde_mt
local function printer(buf, str, len)
	local p = ffi.cast('luact_rbuf_t *', buf)
	p:reserve(len)
	memory.move(p:last_p(), str, len)
	p:use(len)
end
local printer_fn_ptr = ffi.cast('yajl_print_t', printer)
local function create_buf(buf)
	local g = yajl.yajl_gen_alloc(nil)
	yajl.yajl_gen_config(g, ffi.cast('yajl_gen_option', 'yajl_gen_print_callback'), printer_fn_ptr, buf)
	return g
end
function serde_mt.pack_any(buf, obj, len)
	if type(obj) == "number" then
		if math.ceil(obj) == obj then
			yajl.yajl_gen_integer(buf, ffi.new('int64_t', obj))
		else
			yajl.yajl_gen_double(buf, obj)
		end
	elseif type(obj) == "string" then
		yajl.yajl_gen_string(buf, obj, #obj)
	elseif type(obj) == "function" then
		exception.raise('json', 'unsupported', 'streaming unpack have not supported yet')
	elseif type(obj) == "nil" then
		yajl.yajl_gen_null(buf)
	elseif type(obj) == "boolean" then
		yajl.yajl_gen_bool(buf, obj)
	elseif type(obj) == "table" then
		if len or (#obj > 0 and (not obj.__not_array__)) then
			yajl.yajl_gen_array_open(buf)
			for i=1,len or #obj do
				serde_mt.pack_any(buf, obj[i])
			end
			yajl.yajl_gen_array_close(buf)
		else
			yajl.yajl_gen_map_open(buf)
			for k,v in pairs(obj) do
				serde_mt.pack_any(buf, k)
				serde_mt.pack_any(buf, v)
			end
			yajl.yajl_gen_map_close(buf)
		end
	end
end
function serde_mt.pack_vararg(buf, args, len)
	local p = create_buf(buf)
	serde_mt.pack_any(p, args, len)
	local t = type(args)
	if t == 'number' then
		buf:reserve(1)
		buf:last_p()[0] = ('\n'):byte()
		buf:use(1)
	end
	return buf:available()
end
function serde_mt:pack_packet(buf, append, ...)
	local hdsz = ffi.sizeof('luact_writer_raw_t')
	local args = {...}
	if append then
		local sz = buf.used
		--for i=1,#args do
		--	logger.warn('pack2', i, args[i])
		--end
		serde_mt.pack_vararg(buf, args, select('#', ...))
		sz = buf.used - sz
		local pv = ffi.cast('luact_writer_raw_t*', buf:curr_p())
		pv.sz = pv.sz + sz
		--buf:dump()
		return sz
	else
		buf:reserve_with_cmd(hdsz, WRITER_RAW)
		-- allocate size for header (luact_writer_raw_t)
		buf:use(hdsz)
		local sz = buf.used
		--for i=1,#args do
		--	logger.warn('pack', i, args[i])
		--end
		serde_mt.pack_vararg(buf, args, select('#', ...))
		local pv = ffi.cast('luact_writer_raw_t*', buf:curr_p())
		pv.sz = buf.used - sz
		pv.ofs = 0
		--buf:dump()
		return pv.sz
	end
end
function serde_mt:pack(buf, obj)
	return serde_mt.pack_vararg(buf, obj)
end

-- unpack
local stacks = {}
local function get_stack(ctx)
	return stacks[tonumber(ctx.id)]
end

local parse_ctx_mt = {}
parse_ctx_mt.__index = parse_ctx_mt
parse_ctx_mt.NONE = 0
parse_ctx_mt.ARRAY = 1
parse_ctx_mt.MAP = 2
function parse_ctx_mt:fin()
	memory.free(self.cs)
	memory.free(self)
end
function parse_ctx_mt:curstack()
	return self.cs[self.cs_depth]
end
function parse_ctx_mt:in_array()
	return self:curstack().parse_type == self.ARRAY
end
function parse_ctx_mt:in_map()
	return self:curstack().parse_type == self.MAP
end
function parse_ctx_mt:last_key()
	local k = self:curstack().last_k 
	if k ~= nil then
		return ffi.string(k, self:curstack().last_kl)
	else
		return nil
	end
end
function parse_ctx_mt:push_stack(t)
	local st = get_stack(self)
	st[#st + 1] = {}
	if self.cs_size < #st then
		local tmpsz = (self.cs_size * 2)
		local tmp = memory.realloc_typed('struct luact_json_parse_context_stack', self.cs, tmpsz)
		if not tmp then
			exception.raise('melloc', 'struct luact_json_parse_context_stack', tmpsz)
		end
		self.cs = tmp
		self.cs_size = tmpsz
	end
	self.cs_depth = self.cs_depth + 1
	self:set_parse_type(t)
	return 1
end
function parse_ctx_mt:pop_stack()
	local st = get_stack(self)
	local res = table.remove(st)
	self.cs_depth = self.cs_depth - 1
	self:append_value(res)
	return 1
end
function parse_ctx_mt:set_parse_type(t)
	self:curstack().parse_type = t
end
function parse_ctx_mt:set_last_key(k, kl)
	self:curstack().last_k, self:curstack().last_kl = k, kl 
	return 1
end
function parse_ctx_mt:append_value(value)
	local st = get_stack(self)
	local d = self.cs_depth
	if d <= 0 then
		st[1] = value
	elseif self:in_array() then 
		local b = st[d]
		table.insert(b, value)
	elseif self:in_map() then
		local b = st[d]
		b[self:last_key()] = value
	else
		assert(false)
	end
	return 1
end
ffi.metatype('luact_json_parse_context_t', parse_ctx_mt)

local cbs = memory.alloc_typed('yajl_callbacks')
cbs.yajl_null = function (ctx)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:append_value(nil)
end	
cbs.yajl_boolean = function (ctx, bool)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:append_value(bool == 1)
end
cbs.yajl_integer = function (ctx, ll)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:append_value(ll)
end
cbs.yajl_double = function (ctx, d)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:append_value(d)
end
cbs.yajl_number = function (ctx, n, nl)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:append_value(tonumber(ffi.string(n, nl)))
end
cbs.yajl_string = function (ctx, s, sl)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:append_value(ffi.string(s, sl))
end
cbs.yajl_start_map = function (ctx)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:push_stack(ctx.MAP)
end
cbs.yajl_map_key = function (ctx, k, kl)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:set_last_key(k, kl)
end
cbs.yajl_end_map = function (ctx)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:pop_stack()
end
cbs.yajl_start_array = function (ctx)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:push_stack(ctx.ARRAY)
end
cbs.yajl_end_array = function (ctx)
	ctx = ffi.cast('luact_json_parse_context_t*', ctx)
	return ctx:pop_stack()
end

local seed = 0
local function new_parse_context(rb)
	local p = memory.alloc_fill_typed('luact_json_parse_context_t')
	seed = seed + 1
	p.id = seed
	stacks[tonumber(p.id)] = {}
	p.rb = rb
	p.cs_size = 8
	p.cs = memory.alloc_typed('struct luact_json_parse_context_stack', p.cs_size)
	if seed > (20 * 1000 * 1000) then
		seed = 0
	end
	return ffi.gc(p, p.fin)
end
local shared_context = new_parse_context(nil)
local function create_unpacker(ctx)
	return yajl.yajl_alloc(cbs, nil, ctx)
end
function serde_mt.unpack_any(ctx, rb, complete)
	local up = create_unpacker(ctx)
	local r = yajl.yajl_parse(up, rb:curr_p(), rb:available())
	if complete then
		r = yajl.yajl_complete_parse(up)
	end
	if yajl_status_error == r then
		local err = exception.new('json', 'parse_error', 
			ffi.string(yajl.yajl_get_error(up, 1, rb:curr_p(), rb:available())))
		yajl.yajl_free(up)
		return false, err
	end
	local st = get_stack(ctx)
	if #st <= 1 then
		return st[1], 1
	else
		-- TODO : indicate caller that new buffer is needed
		return false, exception.new('json', 'unsupported', 'streaming unpack have not supported yet')
	end
end
function serde_mt:end_stream_unpacker(ctx)
	stacks[tonumber(ctx.id)] = nil
end
function serde_mt:stream_unpacker(rb)
	return new_parse_context(rb)
end
function serde_mt:unpack_packet(ctx)
	return serde_mt.unpack_any(ctx, ctx.rb)
end
function serde_mt:unpack(rb)
	local r = serde_mt.unpack_any(shared_context, rb)
	-- rewind stack
	stacks[tonumber(shared_context.id)] = {}
	return r
end
ffi.metatype('luact_json_serde_t', serde_mt)

return memory.alloc_typed('luact_json_serde_t')
