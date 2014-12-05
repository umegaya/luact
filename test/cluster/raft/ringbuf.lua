local luact = require 'luact.init'
local ringbuf = require 'luact.cluster.raft.ringbuf'

local size = 16
local b = ringbuf.new(size)
assert(b.header.n_size == size)
for i=1,size do
	b:put_at(i, i * 2)
end
assert(b.header.n_size == size)
assert(b:available() == 0)
for i=1,size do
	assert(b:at(i) == i * 2)
end
b:delete(8)
assert(b.header.n_size == size)
assert(b.header.start_idx == 9)
assert(b.header.end_idx == 16)
assert(b:available() == 8)
b:delete(16)
assert(b.header.n_size == size)
assert(b.header.start_idx == 17)
assert(b.header.end_idx == 17)
assert(b:available() == 16)
for i=1,size do
	b:put_at(i+16, i*3)
end
assert(b.header.n_size == size)
assert(b.header.start_idx == 17)
assert(b.header.end_idx == 32)
assert(b:available() == 0)
for i=1,size do
	assert(nil == b:at(i))
end
for i=1,size do
	assert(b:at(i+16) == i*3)
end
for i=1,1 do
	b:put_at(i+32, i*4)
end
assert(b.header.n_size == size*2)
assert(b.header.start_idx == 17)
assert(b.header.end_idx == 33)
for i=2,size do
	b:put_at(i+32, i*4)
end
assert(b.header.n_size == size*2)
assert(b.header.start_idx == 17)
assert(b.header.end_idx == 48)
for i=1,16 do
	assert(b:at(i+16) == i*3)
end
for i=1,16 do
	assert(b:at(i+32) == i*4)
end

return true
