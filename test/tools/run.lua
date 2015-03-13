local data = debug.getinfo(1)
local root = {data.source:find('@(.+)/.+$')}
package.path = root[3]..'/../../luact/lib/?.lua;'..root[3]..'/../../../../luact/lib/?.lua;'..package.path
local term = require 'pulpo.terminal'
local util = require 'pulpo.util'

cmdl = util.luajit_cmdline()
print('cmdl', cmdl)

function run_dir(d)
	local dir = io.popen('ls '..d)
	while true do
		local file = dir:read()
		if not file then break end
		file = (d .. '/' .. file)
		if file:find('%.lua$') then
			term.resetcolor(); print('test: ' .. file .. ' ==========================================')
			local ok, r = pcall(os.execute, cmdl.." test/tools/launch.lua "..file)
			if ok and r then
				if r ~= 0 then
					term.red(); print('test fails:' .. file .. '|' .. r)
					term.resetcolor(); os.exit(-1)
				else
					term.cyan(); print('test: '..file..' OK')
				end
			else
				term.red(); print('execute test fails:' .. file .. '|' .. tostring(r))
				term.resetcolor(); os.exit(-2)
			end
		else
			term.yellow(); print('not test:' .. file .. ' ==========================================')
		end
	end
	term.cyan(); print('test finished')
	term.resetcolor()
end

run_dir('test')
run_dir('test/util')
run_dir('test/storage')
run_dir('test/cluster/raft')
run_dir('test/cluster/gossip')
run_dir('test/cluster/dht')
