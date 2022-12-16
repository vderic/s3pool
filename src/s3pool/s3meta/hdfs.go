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
package s3meta

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"strings"
)

/*
type listRec struct {
	Key  string
	Etag string
}

type listCollection struct {
	Contents []listRec
}
*/

func hdfsListObjects(bucket string, prefix string, notify func(key, etag string)) error {
	var err error

	log.Println("hdfsListObjects", bucket, prefix)

	// invoke gohdfs checksum
	var cmd *exec.Cmd
	if prefix == "" {
		dfspath := "hdfs://" + bucket
		cmd = exec.Command("gohdfs", "checksum", dfspath)
	} else {
		dfspath := "hdfs://" + bucket + "/" + prefix
		cmd = exec.Command("gohdfs", "checksum", dfspath)
	}
	var errbuf bytes.Buffer
	cmd.Stderr = &errbuf
	pipe, _ := cmd.StdoutPipe()
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("gohdfs checksum failed -- %s", string(errbuf.Bytes()))
	}
	defer cmd.Wait()

	// read stdout of cmd
	scanner := bufio.NewScanner(pipe)
	var key string
	var etag string
	for scanner.Scan() {
		s := scanner.Text()
		// Parse s of the form "etag key"
		// Note: the order of Key and ETag is random, but one must follow another.
		nv := strings.SplitN(s, " ", 2)
		if len(nv) != 2 {
			continue
		}

		// extract key value
		etag = strings.Trim(nv[0], " \t")
		key = strings.Trim(nv[1], " \t")
		key = strings.TrimPrefix(key, "/")

		notify(key, etag)
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("gohdfs checksum failed -- %v", err)
	}

	// clean up
	if err = cmd.Wait(); err != nil {
		return fmt.Errorf("gohdfs checksum failed -- %v", err)
	}

	return nil
}
