#!/bin/bash
TYPE=$1
if [ $# -lt 1 ]; then
	TYPE=core
fi
cp luact/lxc/docker/Dockerfile.$TYPE ./Dockerfile
docker build -no-cache=true -t umegaya/luact:$TYPE .
rm Dockerfile
docker push umegaya/luact:$TYPE
