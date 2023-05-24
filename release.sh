#!/bin/bash

if [ $# != 1 ]; then
    echo "usage: $0 version"; echo
    exit 1
fi

url=$(git config --get remote.origin.url)

VERSION=$1
S3POOL=s3pool-$VERSION
echo $S3POOL

rm -rf $S3POOL $S3POOL.zip

git clone $url $S3POOL

find $S3POOL -name .gitignore -delete
rm -rf $S3POOL/.git* 
rm -rf $S3POOL/release.sh 
rm -rf $S3POOL/tests

zip -r $S3POOL.zip $S3POOL
rm -rf $S3POOL
