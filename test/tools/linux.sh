#!/bin/bash
rm -f `pwd`/luact/ext/ffiex/ffiex/cache/*
docker run -ti --rm -v `pwd`:/tmp/test/luact -e LD_PRELOAD=libpthread.so.0 umegaya/luact:core bash -c "/tmp/test/luact/test/tools/test.sh"
