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
package op

import (
	"errors"
	"s3pool/conf"
	"s3pool/jobqueue"
	"s3pool/s3"
	"s3pool/lander"
	"strings"
	"sync"
)

var pullQueue = jobqueue.New(conf.PullConcurrency)

func Pull(args []string) (string, error) {
	conf.CountPull++
	if len(args) < 2 {
		return "", errors.New("Expected at least 2 arguments for PULL")
	}
	schemafn, bucket, keys := args[0], args[1], args[2:]
	if err := checkCatalog(bucket); err != nil {
		return "", err
	}

	nkeys := len(keys)
	path := make([]string, nkeys)
	patherr := make([]error, nkeys)
	waitGroup := sync.WaitGroup{}
	var hit bool

	dowork := func(i int) {
		path[i], hit, patherr[i] = s3.GetObject(bucket, keys[i], false)
		if hit {
			conf.CountPullHit++
			// check the zmp filepath and return to path[i]
			zmppath, err := lander.FindZMPFile(bucket, keys[i])
			if err != nil {
				path[i] = ""
				patherr[i] = errors.New("s3 file cache hit but zmp file not exists")
			} else {
				path[i] = zmppath;
			}


		} else {
			if patherr[i] == nil {
				// check zmp filepath exists. if exists, delete the zmpfile
				zmppath, err := lander.FindZMPFile(bucket, keys[i])
				if err == nil {
					lander.RemoveXrgFile(zmppath)
				}
				// convert path[i] to zmpfile and return to path[i]
				zmppath, err = lander.Csv2Xrg(bucket, keys[i], schemafn)
				if err != nil {
					path[i] = ""
					patherr[i] = err
				} else {
					path[i] = zmppath
				}
			}

		}
		waitGroup.Done()
	}

	// download nkeys in parallel
	waitGroup.Add(nkeys)
	for i := 0; i < nkeys; i++ {
		pullQueue.Add(dowork, i)
	}
	waitGroup.Wait()

	var reply strings.Builder
	for i := 0; i < nkeys; i++ {
		if patherr[i] != nil {
			return "", patherr[i]
		}
		reply.WriteString(path[i])
		reply.WriteString("\n")
	}

	return reply.String(), nil
}
