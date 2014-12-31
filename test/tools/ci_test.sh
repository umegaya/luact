#!/bin/bash
LJBIN="LD_PRELOAD=libpthread.so.0 $1"
if [ $# -lt 1 ]; then
	LJBIN="LD_PRELOAD=libpthread.so.0 luajit"
fi
if [ $# -ge 2 ]; then
	echo "checkout $2"
	git fetch && git checkout $2
fi
echo "exec: $LJBIN test/tool/run.lua"
bash -c "$LJBIN test/tools/run.lua"
