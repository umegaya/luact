local ffi = require 'luact.ffiex'
ffi.cdef [[
#define __asm(exp)
#include <pthread.h>
]]

assert(ffi.C.pthread_join)
