#!/bin/bash
LJBIN=$1
if [ $# -lt 1 ]; then
	LJBIN=luajit
fi
if [ $# -gt 2 ]; then
	git fetch && git checkout $2
fi
$LJBIN test/tools/run.lua
