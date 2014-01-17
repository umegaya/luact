local ffi = require 'luact.ffiex'
ffi.cdef [[
#define __asm(exp)
#include <pthread.h>
#include <memory.h>
#include <stdlib.h>
]]

assert(ffi.C.pthread_join)
assert(ffi.C.malloc)

ffi.path "/usr/local/include/luajit-2.0"
ffi.path "/Applications/Xcode.app/Contents/Developer/usr/lib/llvm-gcc/4.2.1/include"
ffi.cdef "#include <lauxlib.h>"

assert(ffi.C.luaL_newstate, "could not parse lauxlib.h correctly")
