local luact = require 'luact.init'
local msgpack = require 'luact.serde.msgpack'
local uuid = require 'luact.uuid'
local memory = require 'pulpo.memory'
local pbuf = require 'luact.pbuf'
local util = require 'pulpo.util'
local common = require 'luact.serde.common'

ffi.cdef [[
typedef struct long_struct {
	char buffer[100000];
} long_struct_t;
]]
common.register_ctype('struct', 'long_struct', {
	msgpack = {
		packer = function (pack_procs, buf, ctype_id, obj, length, refl)
			local p, ofs = pack_procs.pack_ext_cdata_header(buf, 1, ctype_id)
			p[ofs] = obj.buffer[0]
			return ofs + 1
		end,
		unpacker = function (rb, len)
			local ptr = memory.alloc_typed('long_struct_t')
			ffi.fill(ptr.buffer, 100000, rb:curr_p()[0])
			rb:seek_from_curr(1)
			return ptr
		end,
	},
}, 10000)

local function serde(obj, verify)
	print('serde:test', obj)
	local rb = memory.alloc_typed('luact_rbuf_t')
	rb:init()
	msgpack:pack(rb, obj)
	rb:dump()
	local obj2 = msgpack:unpack(rb)
	if verify then
		assert(verify(obj, obj2))
	else
		logger.report(obj, obj2)
		assert(obj == obj2)
	end
	rb:fin()
	memory.free(rb)
end

serde(nil)
serde(true)
serde(false)
serde(123)
serde("hogefuga")
serde({
	[1] = "fuga",
	[2] = 456,
	[3] = false,
	[4] = {
		1,2,3,4,5,
	}
}, util.table_equals)
serde(function ()
	return 123456
end, function (obj1, obj2)
	return obj1() == obj2()
end)
local id = uuid.first(10, 10)
serde(id, function (obj1, obj2)
	local t, m = uuid.thread_id(obj1), uuid.machine_id(obj1)
	return t == 10 and m == 10 and t == uuid.thread_id(obj2) and m == uuid.machine_id(obj2)
end)
local obj = ffi.new('long_struct_t')
ffi.fill(obj.buffer, 100000, 0xFD)
serde(obj, function (obj1, obj2)
	return memory.cmp(obj1.buffer, obj2.buffer, 100000)
end)


logger.notice('---------------- test streaming unpack -------------------')
local rb = memory.alloc_typed('luact_rbuf_t')
rb:init()
local unp = msgpack:stream_unpacker(rb)

local nLoop = 3000

function makeiary(n)
   local out={}
   for i=1,n do table.insert(out,i) end
   return out
end
function makestr(n)
   local out=""
   for i=1,n-1 do out = out .. "a" end
   out = out .. "b"
   return out
end

local datasets = {
   { "empty", nLoop*1000, {} },
   { "iary1", nLoop*100, {1} },
   { "iary10", nLoop*10, {1,2,3,4,5,6,7,8,9,10} },
   { "iary100", nLoop*1, makeiary(100) },
   { "iary1000", nLoop / 10, makeiary(1000) },
   { "iary10000", nLoop / 10, makeiary(10000) },
   { "str1", nLoop*100, "a" },
   { "str10", nLoop*100,  makestr(10)  },
   { "str100", nLoop*100, makestr(100)  },
   { "str500", nLoop*100, makestr(500)  },   
   { "str1000", nLoop*100, makestr(1000)  },
   { "str10000", nLoop*10, makestr(10000)  },
}

local st, et
_G.BENCH = true
for i,v in ipairs(datasets) do

logger.notice('gc mem', collectgarbage("count"), "KB")

  -- streaming api
  local nLoop = v[2]
  st = os.clock()
  for j=1, nLoop do
  	msgpack:pack(rb, v[3])
    msgpack:unpack_packet(unp)
    rb:shrink_by_hpos()
  end
  et = os.clock()
  local mpstime = et - st
  logger.info('stream: # of iter', nLoop, et - st)

  -- non-streaming
  st = os.clock()
  local offset,res
  for j=1, nLoop do
  	rb:reset()
  	msgpack:pack(rb, v[3])
    offset,res = msgpack:unpack(rb)
  end
  assert(offset)
  local et = os.clock()
  local mptime = et - st
  logger.info('normal: # of iter', nLoop, et - st)
  
--  print( "mp:", v[1], mptime, "sec", "native:", nLoop/mptime, "stream:", nLoop/mpstime, "orig:", nLoop/mpotime, "(times/sec)", (mpotime/mptime), "times faster")
  print( "mp:", v[1], mptime, "sec", "native:", nLoop/mptime, "stream:", nLoop/mpstime )

logger.notice('gc mem after', collectgarbage("count"), "KB")

end

return true
