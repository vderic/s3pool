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

func hdfs2xListObjects(bucket string, prefix string, notify func(key, etag string)) error {
	var err error

	log.Println("hdfs2xListObjects", bucket, prefix)

	// invoke gohdfs checksum
	var cmd *exec.Cmd
	if prefix == "" {
		dfspath := "/" + bucket
		cmd = exec.Command("hadoop", "fs", "-ls", dfspath)
	} else {
		dfspath := "/" + bucket + "/" + prefix
		cmd = exec.Command("hadoop", "fs", "-ls", dfspath)
	}
	var errbuf bytes.Buffer
	cmd.Stderr = &errbuf
	pipe, _ := cmd.StdoutPipe()
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("hadoop fs -ls failed -- %s", string(errbuf.Bytes()))
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
		if strings.HasPrefix(s, "Found") {
			continue
		}

		idx := strings.LastIndex(s, " ")
		if idx == -1 {
			continue
		}

		// extract key value
		etag = "0"
		key = s[idx+1:]
		key = strings.Trim(key, " \t")
		key = strings.TrimPrefix(key, "/"+bucket+"/")

		notify(key, etag)
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("hadoop fs -ls failed -- %v", err)
	}

	// clean up
	if err = cmd.Wait(); err != nil {
		return fmt.Errorf("hadoop fs -ls failed -- %v", err)
	}

	return nil
}
