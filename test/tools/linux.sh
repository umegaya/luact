#!/bin/bash
docker run -ti --rm -v `pwd`:/tmp/luact -e LD_PRELOAD=libpthread.so.0 umegaya/luact:core bash -c "cd /tmp/luact && luajit test/tools/run.lua"

