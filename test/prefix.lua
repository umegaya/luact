local luact = require 'luact.init'
local router = require 'luact.router'
router.DEBUG = true

luact.start({
	datadir = "/tmp/luact",
	n_core = 1, exclusive = true,
}, function ()
	local luact = require 'luact.init'
	local future = require 'luact.future'
	local router = require 'luact.router'
	router.DEBUG = true
	local a1 = luact "./test/tools/ping.lua"
	local a2 = luact "./test/tools/ping.lua"
	local b = luact "./test/tools/test_actor.lua"

	a1:notify_ping(a2, 3) -- will finish a2 finally
	b:sleep(1.0)
	assert(a2:is_finished() and (not a1:is_finished()))

	local ok, r 
	ok, r = pcall(b.timed_sleep, b, 0.5, 1.5)
	assert((not ok) and r:is('actor_timeout'))
	ok, r = pcall(b.timed_sleep, b, 2.5, 1.5)
	assert(ok and r == 3.0)

	local f = future.new(b:async_fuga(2))
	assert(not f:finished())
	ok, r = f:get()
	assert(ok and r == 3)
	
	local f2 = future.new(b:async_sleep(1.5))
	ok, r = f2:get(0.5)
	assert((not ok) and r:is('actor_timeout'))
	ok, r = f2:get(1.5)
	assert(ok and r == 3.0)

	luact.stop()
end)

return true
