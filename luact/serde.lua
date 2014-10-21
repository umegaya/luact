local _M = {}
local kind = {
	serpent = 1,
	json = 2,
	msgpack = 3,
	protobuf = 4,
}
_M.kind = kind

-- serpent
local sp = require 'serpent'
local serpent = {}
_M[kind.serpent] = serpent
function serpent:pack(obj, ptr, wb)
	local str = serpent.dump(obj)
	local data = tostring(#str)+":"+str
	wb:reserve(#data)
	ffi.copy(ptr, data, #data)
	return #str
end
function serpent:unpack(rb)
	local p = rb:curr_p()
	local sz = 0
	while true do
		local ch = p[sz]
		if ch ~= ":" then
			sz = sz + 1
		else
			break
		end
	end
	local dsz = tonumber(ffi.string(p, sz))
	if rb.ofs < (dsz + 1 + sz) then
		-- not enough buffer. keep on reading buffer
		return nil
	else
		local fn, err = loadstring(ffi.string(p + sz + 1, dsz))
		if err then
			-- fatal. invalid record
			return nil, err
		else
			-- can have valid record.
			rb:shrink(sz + 1 + dsz)
			return fn()
		end
	end
end

-- json
local dkjson = require 'dkjson'
local json = {}
_M[kind.json] = json
function json:pack(obj, ptr, wb)
end
function json:unpack(rb)
end

-- msgpack
local mpk = require 'msgpack'
local msgpack = {}
_M[kind.msgpack] = msgpack
function msgpack:pack(obj, ptr, wb)
end
function msgpack:unpack(rb)
end

-- protobuf (TODO)
local protobuf = {}
_M[kind.protobuf] = protobuf
function protobuf:pack(obj, ptr, wb)
end
function protobuf:unpack(rb)
end

return _M
