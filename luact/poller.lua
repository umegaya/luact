-- actor main loop
local ffi = require 'ffiex'
local thread = require 'luact.thread'
local memory = require 'luact.memory'
local util = require 'luact.util'
local fs = require 'luact.fs'
-- ffi.__DEBUG_CDEF__ = true

local _M = {}
local iolist = ffi.NULL
local handlers = {}
local read_handlers, write_handlers, gc_handlers = {}, {}, {}


---------------------------------------------------
-- module body
---------------------------------------------------
function _M.add_handler(reader, writer, gc)
	table.insert(read_handlers, reader)
	table.insert(write_handlers, writer)
	table.insert(gc_handlers, gc)
	return #read_handlers
end

function _M.initialize(opts)
	--> change system limits	
	_M.maxfd = util.maxfd(opts.maxfd or 1024)
	_M.maxconn = util.maxconn(opts.maxconn or 512)
	_M.rmax, _M.wmax = util.setsockbuf(opts.rmax, opts.wmax)

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
		read_handlers = read_handlers, write_handlers = write_handlers, gc_handlers = gc_handlers
	})

	return true
end

function _M.finalize()
	if iolist ~= ffi.NULL then
		memory.free(iolist)
	end
end

function _M.new()
	local p = memory.alloc_typed('luact_poller_t')
	p:init(_M.maxfd)
	return p
end

function _M.newio(fd, type, ctx)
	--for i=0,_M.maxfd-1,1  do
	--	assert(iolist[i].ev.ident == 0ULL, 
	--		"not filled by zero at: "..i.."("..tostring(iolist[i])..")="..tostring(iolist[i].ev.ident))
	--end
	local io = iolist[fd]
	io:init(fd, type, ctx)
	return io
end


return _M
