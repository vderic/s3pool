package s3meta

import (
	"errors"
	"s3pool/conf"
)

func (p *serverCB) list(req *requestType) (reply *replyType) {
	reply = &replyType{}
	if len(req.param) != 2 {
		reply.err = errors.New("LIST requires param (bucket, prefix)")
		return
	}

	bucket, prefix := req.param[0], req.param[1]
	store := getStore(bucket)
	if key, etag, ok := store.retrieve(prefix); ok {
		reply.key = make([]string, len(key))
		copy(reply.key, key)
		reply.etag = make([]string, len(etag))
		copy(reply.etag, etag)
		return
	}

	if conf.DfsMode == conf.DFS_HDFS {
		err := hdfsListObjects(bucket, prefix, func(k, t string) {
			if k[len(k)-1] == '/' {
				// skip DIR
				return
			}
			reply.key = append(reply.key, k)
			reply.etag = append(reply.etag, t)
		})

		if err != nil {
			reply = &replyType{err: err}
			return
		}
	} else if conf.DfsMode == conf.DFS_S3 {
		err := s3ListObjects(bucket, prefix, func(k, t string) {
			if k[len(k)-1] == '/' {
				// skip DIR
				return
			}
			reply.key = append(reply.key, k)
			reply.etag = append(reply.etag, t)
		})

		if err != nil {
			reply = &replyType{err: err}
			return
		}
	} else if conf.DfsMode == conf.DFS_HDFS2X {
		err := hdfs2xListObjects(bucket, prefix, func(k, t string) {
			if k[len(k)-1] == '/' {
				// skip DIR
				return
			}
			reply.key = append(reply.key, k)
			reply.etag = append(reply.etag, t)
		})

		if err != nil {
			reply = &replyType{err: err}
			return
		}
	} else if conf.DfsMode == conf.DFS_LOCAL {
                err := localListObjects(bucket, prefix, func(k, t string) {
                        if k[len(k)-1] == '/' {
                                // skip DIR
                                return
                        }
                        reply.key = append(reply.key, k)
                        reply.etag = append(reply.etag, t)
                })

                if err != nil {
                        reply = &replyType{err: err}
                        return
                }
        }  else if conf.DfsMode == conf.DFS_GCS {
                err := gcsListObjects(bucket, prefix, func(k, t string) {
                        if k[len(k)-1] == '/' {
                                // skip DIR
                                return
                        }
                        reply.key = append(reply.key, k)
                        reply.etag = append(reply.etag, t)
                })

                if err != nil {
                        reply = &replyType{err: err}
                        return
                }
        }


	store.insert(prefix, reply.key, reply.etag)
	return
}
