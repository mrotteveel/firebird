#!/bin/sh
docker run --platform amd64 --rm \
	-e CTNG_UID=${CTNG_UID:-$(id -u)} \
	-e CTNG_GID=${CTNG_GID:-$(id -g)} \
	-v `pwd`/../../../..:/firebird \
	-t --entrypoint /entry-src-bundle.sh asfernandes/firebird-builder:fb6-x64-ng-v1
