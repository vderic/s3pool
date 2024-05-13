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
package gcs

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"s3pool/cat"
	"s3pool/conf"
	//"cloud.google.com/go/storage"
	//"google.golang.org/api/iterator"
)

// Invoke aws s3api to retrieve a file. Form:
//
//	aws s3api get-object --bucket BUCKET --key KEY --if-none-match ETAG tmppath
func GetObject(bucket string, key string, force bool) (retpath string, metapath string, hit bool, err error) {
	if conf.Verbose(1) {
		log.Println("gcs get-objects", bucket, key)
	}

	// Get destination path
	path, err := mapToPath(bucket, key)
	if err != nil {
		err = fmt.Errorf("Cannot map bucket+key to path -- %v", err)
		return
	}

	// Get etag from meta file
	metapath = path + "__meta__"
	etag := extractETag(metapath)
	catetag := cat.Find(bucket, key)

	// check that destination path exists
	if !fileReadable(path) {
		if conf.Verbose(1) {
			log.Println(" ... file does not exist")
		}
		etag = ""
	}

	// If etag did not change, don't go fetch it
	if etag != "" && etag == catetag && !force {
		if conf.Verbose(1) {
			log.Println(" ... cache hit:", key)
		}
		retpath = path
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
	defer os.Remove(tmppath)

	bkt := g_client.Bucket(bucket)
	rc, err := bkt.Object(key).NewReader(g_ctx)
	if err != nil {
		err = fmt.Errorf("gcs error -- %v", err)
		return
	}
	defer rc.Close()

	f, err := os.Create(tmppath)
	if err != nil {
		err = fmt.Errorf("Cannot open temp file for write -- %v", err)
		return
	}

	_, err = io.Copy(f, rc)
	if err != nil {
		return
	}

	// The file has been downloaded to tmppath. Now move it to the right place.
	if err = moveFile(tmppath, path); err != nil {
		return
	}

	gspath := "gs://" + bucket + "/" + key
	etag_content := "0" + " " + gspath
	// Save the meta info
	ioutil.WriteFile(metapath, []byte(etag_content), 0644)

	// Update catalog with the new etag
	etag = extractETag(metapath)
	if etag != "" {
		//log.Println(" ... update", key, etag)
		cat.Upsert(bucket, key, etag)
	}

	// Done!
	retpath = path
	return
}
