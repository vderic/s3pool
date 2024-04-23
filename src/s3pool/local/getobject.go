/*
 *  S3pool - S3 cache on local disk
 *  Copyright (c) 2019 CK Tan
 *  cktanx@gmail.com
 *
 *  S3Pool can be used for free under the GNU General Public License
 *  version 3, where anything released into public must be open source,
 *  or under a commercial license. The commercial license does not
 *  cover derived or ported versions created by third parties under
 *  GPL. To inquire about commercial license, please send email to
 *  cktanx@gmail.com.
 */
package local

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"s3pool/cat"
	"s3pool/conf"
)

var g_src_prefix string

func Init(src_prefix string) {
	g_src_prefix = src_prefix
}

// Invoke aws s3api to retrieve a file. Form:
//
//	aws s3api get-object --bucket BUCKET --key KEY --if-none-match ETAG tmppath
func GetObject(bucket string, key string, force bool) (retpath string, metapath string, hit bool, err error) {
	if conf.Verbose(1) {
		log.Println("local GetObject", bucket, key)
	}

	path, err := mapToPath(bucket, key)
	if err != nil {
		err = fmt.Errorf("Cannot map bucket+key to path -- %v", err)
		return
	}
	// Get etag from meta file
	metapath = path + "__meta__"
	etag := extractETag(metapath)
	catetag := cat.Find(bucket, key)

	// Get destination path
	dfspath := filepath.Join(g_src_prefix, bucket, key)

	// If etag did not change, don't go fetch it
	if etag != "" && etag == catetag && !force {
		if conf.Verbose(1) {
			log.Println(" ... cache hit:", key)
		}
		retpath = dfspath
		hit = true
		return
	}

	if conf.Verbose(1) {
		log.Println(" ... cache miss:", key)
		if catetag == "" {
			log.Println(" ... missing catalog entry")
		}
	}

	// Prepare to write to tmp file
	tmppath, err := mktmpfile()
	if err != nil {
		err = fmt.Errorf("Cannot create temp file -- %v", err)
		return
	}
	os.Remove(tmppath) // avoid File Exists error from hdfs
	defer os.Remove(tmppath)

	// Remote checksum always equals to zero
	newetag := "0"
	if etag == newetag {
		err = nil
		if conf.Verbose(1) {
			log.Println(" ... local file not modified")
		}
		log.Println("   ... etag", etag)
		log.Println("   ... catetag", catetag)
		if etag != catetag && etag != "" {
			log.Println(" ... update", key, etag)
			cat.Upsert(bucket, key, etag)
		}
		retpath = dfspath
		hit = true
		return
	}

	etag_content := "0" + " " + dfspath
	// Save the meta info
	ioutil.WriteFile(tmppath, []byte(etag_content), 0644)
	if err = moveFile(tmppath, metapath); err != nil {
		return
	}

	// Update catalog with the new etag
	etag = extractETag(metapath)
	if etag != "" {
		//log.Println(" ... update", key, etag)
		cat.Upsert(bucket, key, etag)
	}

	// Done!
	retpath = dfspath
	return
}
