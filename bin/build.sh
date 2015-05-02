#!/bin/bash
TYPE=$1
DOCKERFILE=Dockerfile
if [ ! -z $TYPE ]; then
	DOCKERFILE=DOCKERFILE.$TYPE
	TYPE=":$TYPE"
fi
cp luact/lxc/docker/$DOCKERFILE ./Dockerfile
docker build --no-cache=true -t umegaya/luact:$TYPE .
rm Dockerfile
docker push umegaya/luact$TYPE # if no type is provided, generate digest also.

# remove unused image
# docker rmi $(docker images | grep "^<none>" | awk "{print \$3}")
