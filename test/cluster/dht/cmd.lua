local luact = require 'luact.init'
local tools = require 'test.tools.cluster'
local fs = require 'pulpo.fs'

tools.start_luact(1, nil, function ()

local luact = require 'luact.init'
local cmd = require 'luact.cluster.dht.cmd'
local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local range = require 'luact.cluster.dht.range'
local txncoord = require 'luact.storage.txncoord'
local uuid = require 'luact.uuid'
local fixed_msec = 100000000100
function util.clock_pair()
	return math.floor(fixed_msec / 1000), ((fixed_msec % 1000)* 1000)
end
local lamport = require 'pulpo.lamport'
local function hlc(logical_clock)
	return lamport.debug_make_hlc(logical_clock)
end

local g = cmd.get(1, "hoge", 4, hlc(1))
assert(g.kind == 1, "kind should be same as given to ctor")
assert(memory.cmp(g:key(), "hoge", 4), "argument and stored data should be same")

local p = cmd.put(2, "fuga", 4, "baz", 3, hlc(1))
assert(p.kind == 2, "kind should be same as given to ctor")
assert(memory.cmp(p:key(), "fuga", 4), "argument and stored data should be same")
assert(memory.cmp(p:val(), "baz", 3), "argument and stored data should be same")

local c = cmd.cas(3, "fugu", 4, "oldbaz", 6, "newbaz", 6, hlc(1))
assert(c.kind == 3, "kind should be same as given to ctor")
assert(memory.cmp(c:key(), "fugu", 4), "argument and stored data should be same")
assert(memory.cmp(c:oldval(), "oldbaz", 6), "argument and stored data should be same")
assert(memory.cmp(c:newval(), "newbaz", 6), "argument and stored data should be same")

local m = cmd.merge(4, "guha", 4, "barbaz", 10, "cas", 3, hlc(1))
assert(m.kind == 4, "kind should be same as given to ctor")
assert(memory.cmp(m:key(), "guha", 4), "argument and stored data should be same")
assert(memory.cmp(m:val(), "barbaz", 10), "argument and stored data should be same")
assert(memory.cmp(m:op(), "cas", 3), "argument and stored data should be same")

local s = cmd.split(5, range.debug_new(3), range.debug_new(3), hlc(1), txncoord.debug_make_txn({
	kind = 5, key = "A",
	coord = uuid.new(), 
}))
assert(s.kind == 5, "kind should be same as given to ctor")

local uuid = ffi.new('luact_uuid_t')
local w = cmd.watch(6, "guee", 4, uuid, "notify_me", nil, nil, hlc(1))
assert(w.kind == 6, "kind should be same as given to ctor")
assert(memory.cmp(w:key(), "guee", 4), "argument and stored data should be same")
assert(memory.cmp(w:method(), "notify_me", 9), "argument and stored data should be same")
assert(w:arg() == nil, "arg not specified, so should return nil")

local w2 = cmd.watch(7, "guee", 4, uuid, "notify_me", "argument", 8, hlc(1))
assert(w2.kind == 7, "kind should be same as given to ctor")
assert(memory.cmp(w2:key(), "guee", 4), "argument and stored data should be same")
assert(memory.cmp(w2:method(), "notify_me", 9), "argument and stored data should be same")
assert(memory.cmp(w2:arg(), "argument", 8), "arg not specified, so should return nil")

end)

return true
