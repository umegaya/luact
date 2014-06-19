local thread = require 'luact.thread'
local poller = require 'luact.poller'

local NCLIENTS = 1000
local NITER = 100
local opts = {
	maxfd = (2 * NCLIENTS) + 100, -- client / server socket for NCLIENTS + misc
	maxconn = NCLIENTS, 
	cdef_cache_dir = './tmp/cdefs'
}
thread.initialize(opts)
poller.initialize(opts)

local tcp = require 'luact.socket.tcp'

local p = poller.new()
local limit,finish,cfinish = NCLIENTS * NITER,0,0

tcp.listen('0.0.0.0:8888'):by(p, function (s)
	while true do
		local fd = s:read()
		-- print('accept:', fd:fd())
		fd:by(p, function (s)
			local i = 0
			-- print('read start')
			local ptr,len = ffi.new('char[256]')
			while i < NITER do
				len = s:read(ptr, 256)
				-- print('read end', len)
				s:write(ptr, len)
				i = i + 1
				finish = finish + 1
				if (finish % 5000) == 0 then
					io.stdout:write("s")
				end
			end
		end)
	end
end)

local client_msg = ("hello,luact poll"):rep(16)
for i=0,NCLIENTS-1,1 do
	tcp.connect('127.0.0.1:8888'):by(p, function (s)
		local ptr,len = ffi.new('char[256]')
		local i = 0
		while i < NITER do
			-- print('write start:', s:fd())
			s:write(client_msg, #client_msg)
			-- print('write end:', s:fd())
			len = s:read(ptr, 256) --> malloc'ed char[?]
			local msg = ffi.string(ptr,len)
			assert(msg == client_msg, "illegal packet received:"..msg)
			i = i + 1
			cfinish = cfinish + 1
			if (cfinish % 5000) == 0 then
				io.stdout:write("c")
			end
			if cfinish >= limit then
				io.stdout:write("\n")
				p:stop()
			end
		end
	end)
end

print('start', p)
p:start()
assert(limit <= finish and limit <= cfinish, "not all client/server finished but poller terminated")
poller.finalize()
print('success')
