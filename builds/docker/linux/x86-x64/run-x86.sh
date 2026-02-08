#!/bin/sh
mkdir -p `pwd`/../../../../gen
docker run --platform i386 --rm --user `id -u`:`id -g` -v `pwd`/../../../..:/firebird -v `pwd`/../../../../gen:/home/ctng/firebird-build/gen -t firebirdsql/firebird-builder-linux:fb6-x86-ng-v2
