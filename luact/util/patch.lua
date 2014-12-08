-- add or modify ext modules
local ffi = require 'ffiex.init'
local pbuf = require 'luact.pbuf'
local fs = require 'pulpo.fs'
local exception = require 'pulpo.exception'

local C = ffi.C

function fs.load2rbuf(file, rb)
	local alloc, e, r, sz
	if not rb then
		rb = ffi.new('luact_rbuf_t')
		alloc = true
	end
	local fd = C.open(file, fs.O_RDONLY)
	if fd < 0 then
		e = exception.raise('syscall', 'open', ffi.errno(), fd)
		goto ERROR
	end
	sz = C.lseek(fd, 0, fs.SEEK_END)
	if sz < 0 then
		e = exception.raise('syscall', 'lseek', ffi.errno(), fd)
		goto ERROR
	end		
	if C.lseek(fd, 0, fs.SEEK_SET) < 0 then
		e = exception.raise('syscall', 'lseek', ffi.errno(), fd)
		goto ERROR
	end
	rb:reserve(sz)
	r = C.read(fd, rb:start_p(), sz)
	if sz ~= r then
		e = exception.raise('invalid', 'readsize', sz, r)
		goto ERROR
	end
	rb:use(sz)
::ERROR::
	if not e then
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

