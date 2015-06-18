#!/bin/bash
USE_OPKG=
if [ ! -z $USE_OPKG ]; then
	opkg-cl -f /etc/opkg.conf update
	APTGET="opkg-cl -f /etc/opkg.conf install"
	APTREM="opkg-cl -f /etc/opkg.conf remove"
else
	apt-get update
	APTGET="apt-get -y install"
	APTREM="apt-get -y remove"
fi
# install build tools
$APTGET git
$APTGET gcc
$APTGET g++
$APTGET make
$APTGET autoconf
$APTGET automake
$APTGET autotools-dev
$APTGET cmake
$APTGET libtool
$APTGET pkg-config
$APTGET python3.4-dev
$APTGET ruby
$APTGET wget

# install dependency modules
# -- jemalloc : it appears to be not working well with rocksdb or luajit (it may my fault :<), so disabled now.
#$APTGET libjemalloc-dev 
# -- openssl 
$APTGET libssl-dev
pushd /tmp
# -- luajit
git clone http://luajit.org/git/luajit-2.0.git --branch $LUAJIT_VERSION
pushd luajit-2.0
make && make install
popd
rm -rf luajit-2.0
# -- picohttpparser
git clone https://github.com/umegaya/picohttpparser --branch $HTTP_PARSER_VERSION
pushd picohttpparser
make so && objcopy -S libpicohttpparser.so && make install_so
popd
rm -rf picohttpparser
# -- lua cjson
git clone https://github.com/lloyd/yajl --branch $JSON_VERSION
pushd yajl
./configure && make install
popd
rm -rf yajl
# -- rocksdb
git clone https://github.com/facebook/rocksdb.git --branch $ROCKSDB_VERSION
pushd rocksdb
make shared_lib && objcopy -S librocksdb.so && make install
popd
rm -rf rocksdb
popd

# -- docker
$APTGET docker

# -- docker machine
DOCKER_MACHINE_URL=https://github.com/docker/machine/releases/download/$DOCKER_MACHINE_VERSION/docker-machine_linux-amd64
wget $DOCKER_MACHINE_URL -O /usr/local/bin/docker-machine && chmod 755 /usr/local/bin/docker-machine

# cleanup unnecessary modules
$APTREM gcc
$APTREM g++
$APTREM make
$APTREM autoconf
$APTREM automake
$APTREM autotools-dev
$APTREM cmake
$APTREM libtool
$APTREM pkg-config
$APTREM libxml2-dev
$APTREM python3.4-dev
$APTREM ruby
$APTREM wget

if [ ! -z $USE_OPKG ]; then
	rm -rf /var/opkg-lists
else
	apt-get -y autoremove && apt-get -y autoclean && apt-get -y clean
fi
