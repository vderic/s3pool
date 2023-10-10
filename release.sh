if [ $# -eq 0 ]; then
    echo "usage: $0 version [branch]"; echo
    exit 1
fi

BRANCH="main"
VERSION=$1
S3POOL=s3pool-$VERSION
if [ $# -eq 2 ]; then
BRANCH=$2
echo "BRANCH=$BRANCH"
fi

echo $S3POOL
rm -rf $S3POOL $S3POOL.zip
url=$(git config --get remote.origin.url)
git clone $url $S3POOL
if [ $BRANCH != "main" ] ; then
echo "checkout $BRANCH"
(cd $S3POOL && git checkout $BRANCH)
fi

find $S3POOL -name '*.xrg' -delete
find $S3POOL -name '*.csv' -delete
find $S3POOL -name '*.csv.gz' -delete
rm -rf $S3POOL/.git*
rm -rf $S3POOL/release.sh
rm -rf $S3POOL/tests
zip -r $S3POOL.zip $S3POOL
rm -rf $S3POOL

