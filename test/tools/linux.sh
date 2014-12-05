#!/bin/bash

docker run -ti --rm -v `pwd`/luact:/tmp/luact -e LD_PRELOAD=libpthread.so.0 umegaya/pulpo:ubuntu bash -c "cd /tmp/luact && luajit test/tools/run.lua"


