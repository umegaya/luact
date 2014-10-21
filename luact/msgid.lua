local uuid = require 'luact.uuid'
local bit = require 'bit'
local _M = {}

local SERIAL_BIT_SIZE = 32 - uuid.THREAD_BIT_SIZE
local MAX_SERIAL_ID = (bit.lshift(1, SERIAL_BIT_SIZE) - 1)

--[[
 	msgid
--]]
local msgid_index = {}
local msgid_mt = { __index = msgid_index, }

ffi.cdef (([[
	typedef union luact_msgid {
		struct _detail {
			unsigned int thread_id:%d;
			unsigned int serial:%d;
		} detail;
		unsigned int value;
	} luact_msgid_t;

]]):format(uuid.THREAD_BIT_SIZE, SERIAL_BIT_SIZE))

function msgid_index:init()
	self.detail.thread_id = pulpo.thread_id
	self.detail.serial = 0
end

local seed = ffi.new('luact_msgid_t')
function _M.new()
	seed.serial = seed.serial + 1
	if seed.serial >= MAX_SERIAL_ID then
		seed.serial = 1
	end
	return seed.value
end

local tmp = ffi.new('luact_msgid_t')
function _M.thread_id(value)
	tmp.value = value
	return tmp.thread_id
end
function _M.parse(value)
	tmp.value = value
	return tmp.thread_id, tmp.serial
end

return _M
