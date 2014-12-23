local luact = require 'luact.init'
local ringbuf = require 'luact.util.ringbuf'

local size = 16
local b = ringbuf.new(size)
assert(b.header.n_size == size, "internal size should match given initial size")
for i=1,size do
	b:put_at(i, i * 2)
end
assert(b.header.n_size == size, "put should not change intenral size")
assert(b:available() == 0, "available size should reflect actual usage")
for i=1,size do
	assert(b:at(i) == i * 2, "same value as put should be exists")
end
b:delete_range(nil, 8)
assert(b.header.n_size == size, "delete should not change internal size")
assert(b.header.start_idx == 9, "start index should reflect the result of delete")
assert(b.header.end_idx == 16, "end index should reflect the result of delete")
assert(b:available() == 8, "available size should reflect the result of delete")
b:delete_range(nil, 16)
assert(b.header.n_size == size, "internal size should not change if all data is deleted")
assert(b.header.start_idx == 17, "start index should reflect the result of delete")
assert(b.header.end_idx == 17, "end index should reflect the result of delete:"..tostring(b.header.end_idx))
assert(b:available() == 16, "available size should reflect the result of delete")
for i=1,size do
	b:put_at(i+16, i*3)
end
assert(b.header.n_size == size)
assert(b.header.start_idx == 17, "start index should not change by put")
assert(b.header.end_idx == 32, "end index should keep on increasing")
assert(b:available() == 0, "available size should reflect actual usage")
for i=1,size do
	assert(nil == b:at(i), "there should not be no element at already deleted index")
end
for i=1,size do
	assert(b:at(i+16) == i*3, "same value as put should be exists")
end
for i=1,1 do
	b:put_at(i+32, i*4)
end
assert(b.header.n_size == size*2, "size should increase correctly")
assert(b.header.start_idx == 17, "start index should not change by extending size")
assert(b.header.end_idx == 33, "end index should keep on increasing just same as no extend")
for i=2,size do
	b:put_at(i+32, i*4)
end
assert(b.header.n_size == size*2, "available size should not change if its not reach to size limit")
assert(b.header.start_idx == 17, "start index should not change by put")
assert(b.header.end_idx == 48, "end index should keep on increasing")
for i=1,16 do
	assert(b:at(i+16) == i*3, "same value as put should be exists")
end
for i=1,16 do
	assert(b:at(i+32) == i*4, "same value as put should be exists")
end

return true
