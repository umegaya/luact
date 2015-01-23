assert(0 == os.execute(('%s run.lua --cache_dir=/tmp/lunarsurface test/tools/exec.lua'):format(arg[-1])), "no error should occur")
return true
