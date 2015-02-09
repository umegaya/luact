local luact = require 'luact.init'
local sv = require 'luact.supervise'
-- sv.DEBUG = true

luact.start({
	datadir = "/tmp/luact",
	n_core = 1, exclusive = true,
}, function ()
	local luact = require 'luact.init'
	local clock = require 'luact.clock'
	local actor = require 'luact.actor'
	local iter = 10
	math.randomseed(clock.get())

	local function test_proc(err_at)
		local a, svr = luact.supervise "./test/tools/test_actor.lua"
		local est = 1
		for i=1,iter,1 do
		print('=========================== iter ================================', i)
			local ok, r = pcall(a.inc_num, a, err_at)
			if not ok then
				if r:is('actor_error') then
					print('inc_num: scheduled error:', r)
					-- if not ok, a is restarted by supervisor, so est backto 1 (initial value)
					est = 1
				elseif actor.alive(svr) then
					assert(r:is('actor_temporary_fail'), 'unexpected error:'..tostring(r))
				else
					assert(r:is('actor_no_body'), 'unexpected error:'..tostring(r))
				end
			else
				est = est + 1
			end
		end
		while true do
			local ok, r = pcall(a.fuga, a, 0)
			if not ok then
				if actor.alive(svr) then
					assert(r:is('actor_temporary_fail'))
				else
					assert(r:is('actor_no_body'))
					break
				end
			else
				print(r, est)
				assert(r == est)
				break
			end
		end
		if actor.alive(svr) then
			luact.kill(a)
			local ok, r = pcall(a.inc_num, a)
			assert(not ok and r:is('actor_no_body'))
		end
	end

	test_proc(1)
	test_proc(math.random(2, iter - 1))
	test_proc(iter)
	
	luact.stop()
end)

return true
