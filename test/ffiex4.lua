local ffi = require 'luact.ffiex'
ffi.cdef [[
#define __asm(exp)
#include <time.h>
]]

assert(ffi.C.mktime)
