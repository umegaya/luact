#!/bin/bash
# install build tools
apt-get -y install git
apt-get -y install gcc
apt-get -y install g++
apt-get -y install make
apt-get -y install autoconf
apt-get -y install automake
apt-get -y install autotools-dev
apt-get -y install libtool
apt-get -y install pkg-config
apt-get -y install python3.4-dev
apt-get -y install wget

# install dependency modules
# -- jemalloc
apt-get -y install libjemalloc-dev 
# -- openssl 
apt-get -y install libssl-dev
pushd /tmp
# -- luajit
git clone http://luajit.org/git/luajit-2.0.git --branch $LUAJIT_VERSION
pushd luajit-2.0
make && make install
popd
rm -rf luajit-2.0
# -- rocksdb
git clone https://github.com/facebook/rocksdb.git --branch $ROCKSDB_VERSION
pushd rocksdb
make shared_lib && objcopy -S librocksdb.so && make install
popd
rm -rf rocksdb
# -- picohttpparser
git clone https://github.com/umegaya/picohttpparser --branch $HTTP_PARSER_VERSION
pushd picohttpparser
make so && objcopy -S libpicohttpparser.so && make install_so
popd
# -- lua cjson
git clone https://github.com/umegaya/cJSON --branch $JSON_VERSION
pushd lua-cjson
gcc cJSON.c -o libcjson.so -shared -fPIC
install -D -m 755 libcjson.so /usr/local/lib/
popd
popd

# -- docker machine
DOCKER_MACHINE_URL=https://github.com/docker/machine/releases/download/$DOCKER_MACHINE_VERSION/docker-machine_linux-amd64
wget $DOCKER_MACHINE_URL -O /usr/local/bin/docker-machine && chmod 755 /usr/local/bin/docker-machine

# cleanup unnecessary modules
apt-get -y remove gcc
apt-get -y remove g++
apt-get -y remove make
apt-get -y remove autoconf
apt-get -y remove automake
apt-get -y remove autotools-dev
apt-get -y remove libtool
apt-get -y remove pkg-config
apt-get -y remove libxml2-dev
apt-get -y remove python3.4-dev
apt-get -y remove wget
