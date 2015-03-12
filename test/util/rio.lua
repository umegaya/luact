local luact = require 'luact.init'

luact.start({
	datadir = "/tmp/luact",
	n_core = 4, exclusive = true,
}, function ()
local luact = require 'luact.init'
local tools = require 'test.tools.cluster'
if not luact.root then
	logger.error('root not exist')
end
local ok,r = xpcall(function ()
	local clock = require 'luact.clock'
	local actor = require 'luact.actor'
	local rio = require 'luact.util.rio'

	local pulpo = require 'pulpo.init'
	local fs = require 'pulpo.fs'

	local C = (require 'ffiex.init').C

	local thread_id = pulpo.thread_id

	local l = tools.create_latch('test', 4)

	-- add custom message to this node
	function luact.root:remote_io(path)
		return rio.open(path, bit.bor(fs.O_RDONLY))
	end

	-- first thread create work dir
	if thread_id == 1 then
		fs.mkdir('/tmp/luact/rio/')
	end
	local fd = fs.open('/tmp/luact/rio/'..thread_id, bit.bor(fs.O_RDWR, fs.O_TRUNC, fs.O_CREAT), fs.mode("755"))
	local buf = "this is thread"..thread_id
	C.write(fd, buf, #buf)
	C.close(fd)
	l:wait(1)

	local fno = (thread_id % 4) + 1
	local rrio = actor.root_of(nil, fno).require 'luact.util.rio'
	local fd = rrio.file('/tmp/luact/rio/'..fno)

	local rbuf, rsize = fd:read()
	fd:close()
	local s = ffi.string(rbuf.p, rbuf.sz)
	print(s)
	assert(s == 'this is thread'..fno)

	l:wait(2)

end, function (e)
	logger.error(e)
end)

-- fs.rmdir('/tmp/luact/rio/')

luact.stop()
end)

return true
