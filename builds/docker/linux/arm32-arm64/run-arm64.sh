#!/bin/sh
mkdir -p `pwd`/../../../../gen
docker run --platform arm64 --rm --user `id -u`:`id -g` -v `pwd`/../../../..:/firebird -v `pwd`/../../../../gen:/home/ctng/firebird-build/gen -t firebirdsql/firebird-builder-linux:fb6-arm64-ng-v2
