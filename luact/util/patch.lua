-- add or modify ext modules
local ffi = require 'ffiex.init'
local pbuf = require 'luact.pbuf'
local router = require 'luact.router'
local fs = require 'pulpo.fs'
local util = require 'pulpo.util'
local exception = require 'pulpo.exception'
local tentacle = require 'pulpo.tentacle'

local C = ffi.C

local orig_cancel_handler = tentacle.cancel_handler
function tentacle.cancel_handler(obj, co)
	if type(obj) == 'number' then
		router.unregist(obj)
	else
		orig_cancel_handler(obj, co)
	end
end

function util.random_duration(min_sec)
	-- between x msec ~ 2*x msec
	return min_sec + (min_sec * util.random())
end

function fs.load2rbuf(file, rb)
	local alloc, e, r, sz
	if not rb then
		rb = ffi.new('luact_rbuf_t')
		alloc = true
	end
	local fd = C.open(file, fs.O_RDONLY)
	if fd < 0 then
		e = exception.raise('syscall', 'open', fd)
		goto ERROR
	end
	sz = C.lseek(fd, 0, fs.SEEK_END)
	if sz < 0 then
		e = exception.raise('syscall', 'lseek', fd)
		goto ERROR
	end		
	if C.lseek(fd, 0, fs.SEEK_SET) < 0 then
		e = exception.raise('syscall', 'lseek', fd)
		goto ERROR
	end
	rb:reserve(sz)
	r = C.read(fd, rb:start_p(), sz)
	if sz ~= r then
		e = exception.raise('invalid', 'readsize', file, sz, r)
		goto ERROR
	end
	rb:use(sz)
::ERROR::
	if not e then
		if fd >= 0 then
			C.close(fd)
		end
		return rb
	else
		if fd >= 0 then
			C.close(fd)
		end
		if alloc and rb then
			rb:fin()
		end
		if e then e:raise() end
		return nil
	end
end

