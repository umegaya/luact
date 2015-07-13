-- add or modify ext modules
local ffi = require 'ffiex.init'
local pbuf = require 'luact.pbuf'
local router = require 'luact.router'
local clock = require 'luact.clock'
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

local default_retry_opts = {
	wait = 0.1,
	max_wait = 3,
	wait_multiplier = 2,
	max_attempt = 30,
}
local retry_pattern = {
	RESTART = 1,
	CONTINUE = 2,
	ABORT = 3,
	STOP = 4,
}
function util.retry(opts, fn, ...)
	opts = opts and util.merge_table(default_retry_opts, opts) or default_retry_opts
	local n_fail = 0
	local wait_sec = opts.wait
	while true do
		local ok, r, ret = pcall(fn, ...)
		if ok then
			if r == retry_pattern.RESTART then
				n_fail, wait_sec = 0, opts.wait
			elseif r == retry_pattern.CONTINUE then
				n_fail = n_fail + 1
			elseif r == retry_pattern.ABORT then
				logger.report('retry abort')
				return false
			else
				return true, ret
			end
			if n_fail > 0 then	
				if opts.max_attempt > 0 and n_fail >= opts.max_attempt then
					logger.info('reach to max atempt', n_fail, opts.max_attempt)
					return false
				end	
				clock.sleep(wait_sec)
				wait_sec = math.min(opts.max_wait, wait_sec * opts.wait_multiplier)
			end
		else
			-- logger.report('retry fails', r)
			return false, r
		end
	end
end
util.retry_pattern = retry_pattern

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

