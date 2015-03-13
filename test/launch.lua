local data = debug.getinfo(1)
local root = {data.source:find('@(.+)/.+$')}
package.path = root[3]..'/../luact/lib/?.lua;'..root[3]..'/../../../luact/lib/?.lua;'..package.path
local cmdl = (require 'pulpo.util').luajit_cmdline()
assert(0 == os.execute(('%s run.lua --datadir=/tmp/lunarsurface/data test/tools/exec.lua'):format(cmdl)), "no error should occur")
return true
