local luact = require 'luact.init'
local msgpack = require 'luact.serde.msgpack'
local uuid = require 'luact.uuid'
local pbuf = require 'luact.pbuf'
local common = require 'luact.serde.common'
local socket = require 'pulpo.socket'

local memory = luact.memory
local exception = luact.exception
local util = luact.util

local bign = ffi.new('uint64_t', 0x0001349500002001ULL)
local bign2 = ffi.new('uint64_t', 4743754768895923934ULL)
local bign3 = ffi.new('uint64_t', 10141066673075246401ULL)
assert(bign == socket.ntohll(socket.htonll(bign, true), true))
assert(bign2 == socket.ntohll(socket.htonll(bign2, true), true))
assert(bign3 == socket.ntohll(socket.htonll(bign3, true), true))

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
	assert(ffi.typeof(obj1) == ffi.typeof('luact_uuid_t'), tostring(ffi.typeof(obj1)))
	assert(ffi.typeof(obj2) == ffi.typeof('luact_uuid_t'), tostring(ffi.typeof(obj2)))
	local t, m = uuid.thread_id(obj1), uuid.machine_id(obj1)
	return t == 10 and m == 10 and t == uuid.thread_id(obj2) and m == uuid.machine_id(obj2)
end)
local idarray = ffi.new('luact_uuid_t[4]')
for i=0,3 do
	uuid.debug_create_id(111 * (i + 1), 11 * (i + 1), idarray[i])
end
serde(idarray, function (obj1, obj2)
	assert(ffi.typeof(obj1) == ffi.typeof('luact_uuid_t[4]'), tostring(ffi.typeof(obj1)))
	assert(ffi.typeof(obj2) == ffi.typeof('luact_uuid_t[?]'), tostring(ffi.typeof(obj2)))
	assert(ffi.sizeof(obj2) == 48)
	for i=0,3 do
		local m, t = 111 * (i + 1), 11 * (i + 1)
		assert(uuid.machine_id(obj1[i]) == m)
		assert(uuid.machine_id(obj2[i]) == m)
		assert(uuid.thread_id(obj1[i]) == t)
		assert(uuid.thread_id(obj2[i]) == t)
	end
	return true
end)
serde(100000000000ULL)
local obj = ffi.new('long_struct_t')
ffi.fill(obj.buffer, 100000, 0xFD)
serde(obj, function (obj1, obj2)
	return memory.cmp(obj1.buffer, obj2.buffer, 100000)
end)
local e = exception.new('invalid', 'test invalid', 3, 4)
serde(e, function (obj1, obj2)
	assert(obj1:is('invalid') and obj2:is('invalid'))
	return util.table_equals(obj1.args, obj2.args)
end)

local src = [[cb:00:00:00:00:00:a0:63:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:00:1e:41:a5:69:6e:64:65:78:d7:02:01:1a:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:80:41:40:cb:00:00:00:00:00:c0:63:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:40:1e:41:a5:69:6e:64:65:78:d7:02:01:1b:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:00:42:40:cb:00:00:00:00:00:e0:63:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:80:1e:41:a5:69:6e:64:65:78:d7:02:01:1c:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:80:42:40:cb:00:00:00:00:00:00:64:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:c0:1e:41:a5:69:6e:64:65:78:d7:02:01:1d:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:00:43:40:cb:00:00:00:00:00:20:64:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:00:1f:41:a5:69:6e:64:65:78:d7:02:01:1e:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:80:43:40:cb:00:00:00:00:00:40:64:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:40:1f:41:a5:69:6e:64:65:78:d7:02:01:1f:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:00:44:40:cb:00:00:00:00:00:60:64:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:80:1f:41:a5:69:6e:64:65:78:d7:02:01:20:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:80:49:40:cb:00:00:00:00:00:c0:65:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:20:20:41:a5:69:6e:64:65:78:d7:02:01:21:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:00:4a:40:cb:00:00:00:00:00:e0:65:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:40:20:41:a5:69:6e:64:65:78:d7:02:01:22:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:80:4a:40:cb:00:00:00:00:00:00:66:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:60:20:41:a5:69:6e:64:65:78:d7:02:01:23:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:00:4b:40:cb:00:00:00:00:00:20:66:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:80:20:41:a5:69:6e:64:65:78:d7:02:01:24:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:80:4b:40:cb:00:00:00:00:00:40:66:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:a0:20:41:a5:69:6e:64:65:78:d7:02:01:25:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:00:4c:40:cb:00:00:00:00:00:60:66:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:c0:20:41:a5:69:6e:64:65:78:d7:02:01:26:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:80:4c:40:cb:00:00:00:00:00:80:66:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:e0:20:41:a5:69:6e:64:65:78:d7:02:01:27:00:00:00:00:00:00:00:84:a4:74:65:72:6d:d7:02:01:01:00:00:00:00:00:00:00:a3:6c:6f:67:92:cb:00:00:00:00:00:00:4d:40:cb:00:00:00:00:00:a0:66:40:a5:6d:73:67:69:64:cb:00:00:00:00:00:00:21:41:]]
local data, len = memory.alloc_typed('char', #src / 3), 0
for x in src:gmatch('%w%w') do
	data[len] = tonumber(x, 16)
	len = len + 1
end
local rbtest = memory.alloc_typed('luact_rbuf_t')
rbtest:init()
rbtest:reserve(len)
ffi.copy(rbtest:curr_p(), data, len)
rbtest:use(len)
rbtest:dump()
local sup = msgpack:stream_unpacker(rbtest)
while true do
	local r = msgpack:unpack_packet(sup)
	if not r then
		break
	end
end
assert(rbtest.used == rbtest.hpos)


os.exit(0)

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
   { "iary1", nLoop*1000, {1} },
   { "iary10", nLoop*100, {1,2,3,4,5,6,7,8,9,10} },
   { "iary100", nLoop*10, makeiary(100) },
   { "iary1000", nLoop, makeiary(1000) },
   { "iary10000", nLoop, makeiary(10000) },
   { "str1", nLoop*100, "a" },
   { "str10", nLoop*100,  makestr(10)  },
   { "str100", nLoop*100, makestr(100)  },
   { "str500", nLoop*100, makestr(500)  },   
   { "str1000", nLoop*100, makestr(1000)  },
   { "str10000", nLoop*10, makestr(10000)  },
}

logger.notice('---------------- bench normal unpack -------------------')

local st, et
_G.BENCH = true
for i,v in ipairs(datasets) do

logger.notice('gc mem', collectgarbage("count"), "KB")

   local nLoop = v[2]
  -- streaming api
 --[[ 
  st = os.clock()
  for j=1, nLoop do
  	rb:reset()
  	msgpack:pack(rb, v[3])
    msgpack:unpack_packet(unp)
    rb:shrink_by_hpos()
  end
  et = os.clock()
  local mpstime = et - st
  logger.info('stream: # of iter', nLoop, et - st)
  ]]

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
  print( "mp:", v[1], mptime, "sec", "native:", nLoop/mptime)--, "stream:", nLoop/mpstime )

logger.notice('gc mem after', collectgarbage("count"), "KB")

end

logger.notice('---------------- bench streaming unpack -------------------')

for i,v in ipairs(datasets) do

logger.notice('gc mem', collectgarbage("count"), "KB")

   local nLoop = v[2]
  -- streaming api
  st = os.clock()
  for j=1, nLoop do
  	msgpack:pack(rb, v[3])
	assert(msgpack:unpack_packet(unp))
    rb:shrink_by_hpos()
  end
  et = os.clock()
  local mpstime = et - st
  logger.info('stream: # of iter', nLoop, et - st)

  
--  print( "mp:", v[1], mptime, "sec", "native:", nLoop/mptime, "stream:", nLoop/mpstime, "orig:", nLoop/mpotime, "(times/sec)", (mpotime/mptime), "times faster")
  print( "mp:", v[1], mpstime, "sec", "stream:", nLoop/mpstime)--, "stream:", nLoop/mpstime )

logger.notice('gc mem after', collectgarbage("count"), "KB")

end


return true
