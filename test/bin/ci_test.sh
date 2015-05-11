#!/bin/bash
LJBIN="LD_PRELOAD=libpthread.so.0 luajit-2.1.0-alpha -e \"require('jit.opt').start('minstitch=10000')\""
if [ $# -ge 1 ]; then
	echo "checkout $1"
	git fetch && git checkout origin/$1 && git submodule update --recursive
fi
echo "exec: $LJBIN test/tools/run.lua"
bash -c "$LJBIN test/tools/run.lua"
