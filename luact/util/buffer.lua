local memory = require 'pulpo.memory'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'

local _M = {}

local buffer_mt = {}
--[[
	=>: prev, <=: next
	ff:ff:ff:ff 		<=> ff:ff:ff:fe:ff...
	ff:ff:ff:01 		<=> ff:ff:ff:00:ff...
	ff:ff:fe:ff... 		<=> ff:ff:fe:ff...:fe
	ff:ff:fe:ff...:00 	<=> ff:ff:fe:ff...(remove trailing zero)
	ff:ff:fe:ff... (max length - 1) <=> ff:ff:fe:ff:...:fe:ff (max length)
	01:00...			<=> 01:00... (length - 1)
	00...				<=> 00... (length - 1)
	00 					<=> ""
]]
function buffer_mt:prev(buf)
	self:copy_to(buf)
	buf:dec()
	return buf
end
function buffer_mt:dec()
	local idx, v = self:length() - 1
	if idx >= 0 then
		local v = self:value_ptr()[idx]
		if v > 0 then
			self:value_ptr()[idx] = v - 1
			ffi.fill(self:value_ptr() + idx + 1, self:max_length() - idx - 1, 0xFF)
			self:set_length(self:max_length())
		else -- remove trail 0 
			self:set_length(self:length() - 1)
		end
	else
		exception.raise('invalid', 'this is min key')
	end
	return self
end
function buffer_mt:next(buf)
	self:copy_to(buf)
	buf:inc()
	return buf
end

function buffer_mt:inc()
	if self:length() < self:max_length() then
		-- add trailing zero
		self:value_ptr()[self:length()] = 0x00
		self:set_length(self:length() + 1)
		return self
	end
	local idx, v = self:length() - 1
::AGAIN::
	v = self:value_ptr()[idx]
	if v < 0xFF then
		self:value_ptr()[idx] = v + 1
		-- fill by zero at the end of key buffer
		self:set_length(idx + 1)
	elseif idx > 0 then
		idx = idx - 1
		goto AGAIN
	else -- all bytes are 0xFF
		exception.raise('invalid', 'this is max key')		
	end
	return self
end
function buffer_mt:equals(k, kl)
	if self:length() ~= kl then
		return false
	else
		return memory.cmp(self:value_ptr(), k, kl)
	end
end
function buffer_mt:less_than(k, kl)
	local len = self:length()
	if len <= 0 then -- min key
		return kl > 0
	elseif kl <= 0 then -- k is min key
		return false
	elseif len < kl then
		return memory.rawcmp(self:value_ptr(), k, len) <= 0 
	elseif len >= kl then
		return memory.rawcmp(self:value_ptr(), k, kl) < 0
	end
end
function buffer_mt:less_than_equals(k, kl)
	local len = self:length()
	if len <= 0 then -- min key
		return true
	elseif kl <= 0 then -- k is min key
		return len <= 0
	elseif len <= kl then
		return memory.rawcmp(self:value_ptr(), k, len) <= 0 
	elseif len > kl then
		return memory.rawcmp(self:value_ptr(), k, kl) < 0
	end
end
function buffer_mt:__eq(key)
	return self:equals(key:value_ptr(), key:length())
end
function buffer_mt:__lt(key)
	return self:less_than(key:value_ptr(), key:length())
end
function buffer_mt:__le(key)
	return self:less_than_equals(key:value_ptr(), key:length())
end
function buffer_mt:__tostring()
	local len = tonumber(self:length())
	local s = "#"..tostring(len)
	for i=0,len-1 do
		s=s..(":%02x"):format(self:value_ptr()[i])
	end
	return s
end
function buffer_mt:as_digest()
	local len = tonumber(self:length())
	local dlen = math.min(16, len)
	local s = "#"..tostring(len)
	for i=0,dlen-1 do
		s=s..(":%02x"):format(self:value_ptr()[i])
	end
	if self:length() > dlen then
		s=s..("...")
	end
	return s
end
function buffer_mt:as_slice()
	return self:value_ptr(), self:length()
end

function _M.new_mt()
	local mt = util.copy_table(buffer_mt)
	mt.__index = mt
	return mt
end

return _M
