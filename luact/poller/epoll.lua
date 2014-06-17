local opts = ...
local thread = require 'luact.thread'
local util = require 'luact.util'

local _M = {}
local handlers = opts.handlers
local read_handlers, write_handlers, gc_handlers = 
	opts.read_handlers, opts.write_handlers, opts.gc_handlers

---------------------------------------------------
-- import necessary cdefs
---------------------------------------------------
thread.import("epoll.lua", {
	"epoll_create", "epoll_wait", "epoll_ctl",
}, {
	"EPOLL_CTL_ADD", "EPOLL_CTL_MOD", "EPOLL_CTL_DEL",
	"EPOLLIN", "EPOLLOUT", "EPOLLRDHUP", "EPOLLPRI", "EPOLLERR", "EPOLLHUP",
	"EPOLLET", "EPOLLONESHOT"  
}, nil, [[
	#include <sys/epoll.h>
]])
