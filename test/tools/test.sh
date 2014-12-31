#!/bin/bash
LJBIN=$1
if [ $# -lt 1 ]; then
	LJBIN=luajit
fi
cp /luact/luact/ext/ffiex/ffiex/cache/* /tmp/test/luact/luact/ext/ffiex/ffiex/cache/
cd /tmp/test/luact
$LJBIN test/tools/run.lua
