#!/bin/sh
docker buildx build \
	--progress=plain \
	--pull \
	--build-arg ARG_BASE=i386/debian:bookworm \
	--build-arg ARG_SET_ARCH=i686 \
	--build-arg ARG_TARGET_ARCH=i586-pc-linux-gnu \
	--build-arg ARG_CTNF_CONFIG=crosstool-ng-config-x86 \
	-t firebirdsql/firebird-builder-linux:fb6-x86-ng-v2 .
