assert(0 == os.execute(('%s run.lua --datadir=/tmp/lunarsurface/data test/tools/exec.lua'):format(arg[-1])), "no error should occur")
return true
