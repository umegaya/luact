-- actor main loop
local ffi = require 'ffiex'
local thread = require 'luact.thread'
local memory = require 'luact.memory'
local util = require 'luact.util'
local fs = require 'luact.fs'
local signal = require 'luact.signal'
-- ffi.__DEBUG_CDEF__ = true

local _M = {}
local iolist = ffi.NULL
local handlers = {}
local read_handlers, write_handlers, gc_handlers, error_handlers = {}, {}, {}, {}


---------------------------------------------------
-- module body
---------------------------------------------------
function _M.add_handler(reader, writer, gc, err)
	table.insert(read_handlers, reader)
	table.insert(write_handlers, writer)
	table.insert(gc_handlers, gc)
	table.insert(error_handlers, err)
	return #read_handlers
end

function _M.initialize(opts)
	--> change system limits	
	_M.maxfd = util.maxfd(opts.maxfd or 1024)
	_M.maxconn = util.maxconn(opts.maxconn or 512)
	_M.rmax, _M.wmax = util.setsockbuf(opts.rmax, opts.wmax)

	--> tweak signal handler
	signal.ignore("SIGPIPE")

	-- system dependent initialization (it should define luact_poller_t, luact_io_t)
	local poller = opts.poller or (
		ffi.os == "OSX" and 
			"kqueue" or 
		(ffi.os == "Linux" and 
			"epoll" or 
			assert(false, "unsupported arch:"..ffi.os))
	)
	iolist = require ("luact.poller."..poller).initialize({
		opts = opts,
		poller = _M, 
		handlers = handlers,
		read_handlers = read_handlers, 
		write_handlers = write_handlers, 
		gc_handlers = gc_handlers, 
		error_handlers = error_handlers,
	})

	return true
end

function _M.finalize()
	if iolist ~= ffi.NULL then
		memory.free(iolist)
	end
	for _,p in ipairs(_M.pollerlist) do
		p:fin()
		memory.free(p)
	end
end

_M.pollerlist = {}
function _M.new()
	local p = memory.alloc_typed('luact_poller_t')
	p:init(_M.maxfd)
	table.insert(_M.pollerlist, p)
	return p
end

function _M.newio(fd, type, ctx)
	local io = iolist[fd]
	io:init(fd, type, ctx)
	return io
end


return _M
