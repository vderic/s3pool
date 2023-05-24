#!/bin/bash

source ./common.sh

clear_s3pool
start_s3pool -s3

bucket=vitessedata-public
key="tpch/sf1/csv/lineitem*"

info "Test GLOB "
echo "[\"GLOB\",\"${bucket}\",\"${key}\"]" \
	| nc ${host} ${port} 


key="tpch/sf1/csv/lineitem.prt.18814184.csv.gz"
schema=$(realpath ~/p/s3pool/tests/lineitem.schema)
json='{\"fmt\":\"csv\",\"csvspec\":{\"delim\":\"|\",\"quote\":\"\\\"\",\"escape\":\"\\\"\",\"header_line\":false,\"nullstr\":\"\"}}'

info "Test PULL "
echo "[\"PULL\",\"${json}\",\"${schema}\",\"${bucket}\",\"${key}\"]" \
	| nc ${host} ${port} 

