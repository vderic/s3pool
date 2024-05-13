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
	"cloud.google.com/go/storage"
	"context"
	"google.golang.org/api/iterator"
)

var g_ctx context.Context
var g_client *storage.Client = nil

type ListRecord struct {
	Key  string
	Etag string
}

type ListCollection struct {
	Contents []ListRecord
}

func gcsListObjects(bucket string, prefix string, notify func(key, etag string)) error {
	var err error = nil

	if g_client == nil {
		g_ctx = context.Background()
		g_client, err = storage.NewClient(g_ctx)
		if err != nil {
			return err
		}
	}

	bkt := g_client.Bucket(bucket)
	query := &storage.Query{Prefix: prefix}
	query.SetAttrSelection([]string{"Name"})

	it := bkt.Objects(g_ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		key := attrs.Name
		etag := ""
		notify(key, etag)
	}

	return err
}
