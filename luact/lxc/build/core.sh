#!/bin/bash
# initializing main repo
LUACT_VERSION=master
git clone https://github.com/umegaya/luact.git --branch=$LUACT_VERSION
pushd luact
sed -i 's/git@github.com:/https:\/\/github.com\//' .gitmodules
git submodule update --init --recursive
# Initialize ffiex
apt-get -y install gcc
apt-get -y install g++
luajit -e "(require 'luact.init').init_cdef_cache()"
apt-get -y remove gcc
apt-get -y remove g++
popd
# for installing necessary system headers
apt-get install -y libc6-dev
apt-get install -y libgcc-4.9-dev
