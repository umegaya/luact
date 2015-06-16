local data = debug.getinfo(1)
local root = {data.source:find('@(.+)/.+$')}
package.path = root[3]..'/../luact/lib/?.lua;'..root[3]..'/../../../luact/lib/?.lua;'..package.path
local cmdl = (require 'pulpo.util').luajit_cmdline()
local env = "--startup_at=1234567890 --parent_address=167772163"
assert(0 == os.execute(('LUACT_OPTIONS=\'%s\' %s run.lua --datadir=/tmp/lunarsurface/data test/tools/exec.lua'):format(env, cmdl)), "no error should occur")
return true
