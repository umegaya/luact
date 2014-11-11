#!/bin/bash

docker run -ti --rm -v /Users/iyatomi/Documents/umegaya/luact:/tmp/luact -e LD_PRELOAD=libpthread.so.0 umegaya/pulpo:ubuntu bash -c "cd /tmp/luact && luajit test/tools/run.lua"


