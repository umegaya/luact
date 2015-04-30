#!/bin/bash
# initializing main repo
LUACT_VERSION=umegaya/feature/vid
git clone https://github.com/umegaya/luact.git --branch=$LUACT_VERSION
pushd luact
sed -i 's/git@github.com:/https:\/\/github.com\//' .gitmodules
git submodule update --init --recursive
# update foo repository again
pushd test/deploy/foo
sed -i 's/git@github.com:/https:\/\/github.com\//' .gitmodules
git submodule sync
git submodule update --init --recursive
pushd bar
sed -i 's/git@github.com:/https:\/\/github.com\//' .gitmodules
git submodule sync
git submodule update --init --recursive
popd
popd
# Initialize ffiex
apt-get -y install gcc
apt-get -y install g++
luajit-2.1.0-alpha -e "require('jit.opt').start('minstitch=10000');(require 'luact.init').init_cdef_cache()"
apt-get -y remove gcc
apt-get -y remove g++
popd
# for installing necessary system headers
apt-get install -y libc6-dev
apt-get install -y libgcc-4.9-dev
