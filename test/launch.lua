assert(0 == os.execute(('%s run.lua test/tools/exec.lua'):format(arg[-1])), "no error should occur")
return true
