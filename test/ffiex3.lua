package.path = package.path .. ";src/?.lua"
local ffi = require 'src/ffiex'
ffi.cdef [[
#include <memory.h>
]]
