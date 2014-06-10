local thread = require 'luact.thread'
local poller = require 'luact.poller'
local tcp = require 'luact.socket.tcp'

local opts = {
	cdef_cache_dir = './tmp/cdefs'
}
thread.initialize(opts)
poller.initialize(opts)

local p = poller.new()
local limit,finish,cfinish = 1000,0,0

tcp.listen('0.0.0.0:8888'):by(p, function (s)
	while true do
		local fd = s:read() --> malloc'ed int[?]
		fd:by(p, function (s)
			s:write(s:read())
			finish = finish + 1
		end)
	end
end)

local client_msg = "hello, poller"
for i=0,limit-1,1 do
	tcp.connect('localhost:8888'):by(p, function (s)
		local ptr,len = ffi.new('char[256]')
		s:write(client_msg)
		len = s:read(ptr, 256) --> malloc'ed char[?]
		local msg = ffi.string(ptr,len)
		assert(msg == client_msg, "illegal packet received:"..msg)
		cfinish = cfinish + 1
		if cfinish >= limit then
			p:stop()
		end
	end)
end

p:start()
assert(limit <= finish and limit <= cfinish, "not all client/server finished but poller terminated")
