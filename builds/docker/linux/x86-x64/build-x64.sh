#!/bin/sh
docker buildx build \
	--progress=plain \
	--pull \
	--build-arg ARG_BASE=debian:bookworm \
	--build-arg ARG_SET_ARCH=x86_64 \
	--build-arg ARG_TARGET_ARCH=x86_64-pc-linux-gnu \
	--build-arg ARG_CTNF_CONFIG=crosstool-ng-config-x64 \
	-t firebirdsql/firebird-builder-linux:fb6-x64-ng-v2 .
