#!/bin/bash -eu
EMAIL=`git config --global user.email`
NAME=`git config --global user.name`
if [[ -z $EMAIL || -z $NAME ]]; then
	git config --global user.email "you@example.com"
	git config --global user.name "Your Name"
fi

pushd ./test/deploy/foo
	git reset --hard 4f4dc0f06330f77e1a7f55db1249d7502ee51f15
	pushd ./bar
		git reset --hard 6dd51173d85ee4c25893226e6d8e5435fa739202
		pushd ./baz
			git reset --hard 01d898ef94899c6312c4ac3b8bc455e7a997335a
			if [ -z "$1" ]; then
				sed -i.bk "s/ver2/ver3/g" init.lua
				rm init.lua.bk
				git commit -a -m "test commit"
			fi
		popd
		if [ -z "$1" ]; then
			git commit -a -m "test commit"
		fi
	popd
	if [ -z "$1" ]; then
		git commit -a -m "test commit"
	fi
popd
