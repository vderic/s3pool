#!/bin/bash

### Get current directory
DIR=$(dirname ${BASH_SOURCE[0]})
DIR=$(realpath ${DIR})

### s3pool options
s3pool=~/p/s3pool/src/s3pool/s3pool
homedir=/tmp/s3pool_home
devdir=/tmp/s3pool_xrg
host=localhost
port=8989

### Add xrgdiv path 
export PATH=$PATH:~/p/lander/src

function info() {
	echo "##########" $*
}

function clear_s3pool() {
	info "Kill s3pool and clear cache"
	pkill s3pool
	rm -rf ${homedir}/* ${devdir}/*
}

function start_s3pool() {
	mode=${1:-s3}
	info "Start s3pool"
	${s3pool} -D ${homedir} -d ${devdir} -p ${port} ${mode}
	echo "pidof s3pool = $(pidof s3pool)"
	sleep 1
}

