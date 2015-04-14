#!/bin/bash -eu
pushd ./test/deploy/foo
	git reset --hard 6b61b019fba690dc4dbda7f90bde71969fa39a4f
	pushd ./bar
		git reset --hard 206f5eb40817133469bddad419dd87323890e305
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
