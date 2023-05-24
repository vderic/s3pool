#!/bin/bash

mode=-local

source ./common.sh

clear_s3pool
start_s3pool -local 

bucket=$(echo ${DIR} | cut -d'/' -f2) 
tmp=$(echo ${DIR} | cut -d '/' -f3-)
key=${tmp}/lineitem*.gz

info "Test GLOB "
echo "[\"GLOB\",\"${bucket}\",\"${key}\"]" \
	| nc ${host} ${port} 

key=${tmp}/lineitem.prt.18814184.csv.gz
schema=$(realpath ~/p/s3pool/tests/lineitem.schema)
json='{\"fmt\":\"csv\",\"csvspec\":{\"delim\":\"|\",\"quote\":\"\\\"\",\"escape\":\"\\\"\",\"header_line\":false,\"nullstr\":\"\"}}'

info "Test PULL "
echo "[\"PULL\",\"${json}\",\"${schema}\",\"${bucket}\",\"${key}\"]" \
	| nc ${host} ${port} 

