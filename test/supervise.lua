local luact = require 'luact.init'

luact.start({
	cache_dir = "/tmp/luact",
	n_core = 1, exclusive = true,
}, function ()
	local luact = require 'luact.init'
	local clock = require 'luact.clock'
	local a, svr = luact.supervise "./test/tools/test_actor.lua"
	local iter = 10
	math.randomseed(clock.get())
	local est, err_at = 1, math.random(1, iter)
	for i=1,iter,1 do
	print('=========================== iter ================================', i)
		local ok, r = pcall(a.inc_num, a, err_at)
		if not ok then
			if r:is('actor_runtime_error') then
				print('inc_num: scheduled error:', r)
				-- if not ok, a is restarted by supervisor, so est backto 1 (initial value)
				est = 1
			else
				assert(r:is('actor_temporary_fail'), 'unexpected error:'..tostring(r))
			end
		else
			est = est + 1
		end
	end
	assert(a:fuga(0) == est)
	luact.kill(a)
	local ok, r = pcall(a.inc_num, a)
	assert(not ok and r:is('actor_no_body'))

	luact.stop()
end)

return true
