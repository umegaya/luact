local luact = require 'luact.init'
local cmd = require 'luact.cluster.dht.cmd'
local memory = require 'pulpo.memory'

local g = cmd.get(1, "hoge", 4)
assert(g.kind == 1, "kind should be same as given to ctor")
assert(memory.cmp(g:key(), "hoge", 4), "argument and stored data should be same")

local p = cmd.put(2, "fuga", 4, "baz", 3)
assert(p.kind == 2, "kind should be same as given to ctor")
assert(memory.cmp(p:key(), "fuga", 4), "argument and stored data should be same")
assert(memory.cmp(p:val(), "baz", 3), "argument and stored data should be same")

local c = cmd.cas(3, "fugu", 4, "oldbaz", 6, "newbaz", 6)
assert(c.kind == 3, "kind should be same as given to ctor")
assert(memory.cmp(c:key(), "fugu", 4), "argument and stored data should be same")
assert(memory.cmp(c:oldval(), "oldbaz", 6), "argument and stored data should be same")
assert(memory.cmp(c:newval(), "newbaz", 6), "argument and stored data should be same")

local m = cmd.merge(4, "guha", 4, "cas barbaz", 10)
assert(m.kind == 4, "kind should be same as given to ctor")
assert(memory.cmp(m:key(), "guha", 4), "argument and stored data should be same")
assert(memory.cmp(m:val(), "cas barbaz", 10), "argument and stored data should be same")

local s = cmd.split(5, "gyaa", 4)
assert(s.kind == 5, "kind should be same as given to ctor")
assert(memory.cmp(s:key(), "gyaa", 4), "argument and stored data should be same")

local uuid = ffi.new('luact_uuid_t')
local w = cmd.watch(6, "guee", 4, uuid, "notify_me")
assert(w.kind == 6, "kind should be same as given to ctor")
assert(memory.cmp(w:key(), "guee", 4), "argument and stored data should be same")
assert(memory.cmp(w:method(), "notify_me", 9), "argument and stored data should be same")
assert(w:arg() == nil, "arg not specified, so should return nil")

local w2 = cmd.watch(7, "guee", 4, uuid, "notify_me", "argument", 8)
assert(w2.kind == 7, "kind should be same as given to ctor")
assert(memory.cmp(w2:key(), "guee", 4), "argument and stored data should be same")
assert(memory.cmp(w2:method(), "notify_me", 9), "argument and stored data should be same")
assert(memory.cmp(w2:arg(), "argument", 8), "arg not specified, so should return nil")

return true