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
package hdfs2x

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func mktmpfile() (path string, err error) {
	fp, err := ioutil.TempFile("tmp", "dfs_")
	if err != nil {
		return
	}
	defer fp.Close()
	path, err = filepath.Abs(fp.Name())
	return

}

// move file src to dst while ensuring that
// the dst's dir is created if necessary
func moveFile(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}

	idx := strings.LastIndexByte(dst, '/')
	if idx > 0 {
		dirpath := dst[:idx]
		if err := os.MkdirAll(dirpath, 0755); err != nil {
			return fmt.Errorf("Cannot mkdir %s -- %v", dirpath, err)
		}
	}

	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("Cannot mv file -- %v", err)
	}

	return nil
}

// Read the ETag entry from a FNAME__meta__ file
func extractETag(path string) string {
	byt, err := ioutil.ReadFile(path)
	if err != nil {
		return ""
	}

	dat := string(byt)
	nv := strings.SplitN(dat, " ", 2)
	if len(nv) != 2 {
		return ""
	}

	etag := nv[0]
	return etag
}

func mapToPath(bucket, key string) (path string, err error) {
	path, err = filepath.Abs(fmt.Sprintf("data/%s/%s", bucket, key))
	return
}

func fileReadable(path string) bool {
	f, err := os.Open(path)
	if err == nil {
		f.Close()
	}
	return err == nil
}
