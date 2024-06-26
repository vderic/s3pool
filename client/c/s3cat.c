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
#define _XOPEN_SOURCE 500
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "s3pool.h"

void usage(const char* pname, const char* msg)
{
	fprintf(stderr, "Usage: %s [-h] -p port schenafn bucket key [key...]\n", pname);
	fprintf(stderr, "Copy s3 files to stdout.\n\n");
	fprintf(stderr, "    -p port : specify the port number of s3pool process\n");
	fprintf(stderr, "    -h      : print this help message\n");
	fprintf(stderr, "\n");
	if (msg) {
		fprintf(stderr, "%s\n\n", msg);
	}

	exit(msg ? -1 : 0);
}


void fatal(const char* msg)
{
	fprintf(stderr, "FATAL: %s\n", msg);
	exit(1);
}


void catfile(const char* fname)
{
	FILE* fp = fopen(fname, "r");
	if (!fp) {
		perror("fopen");
		exit(1);
	}

	while (1) {
		char buf[100];
		int n = fread(buf, 1, sizeof(buf), fp);
		if (ferror(fp)) {
			perror("fread");
			exit(1);
		}
		if (n != (int) fwrite(buf, 1, n, stdout)) {
			perror("fwrite");
			exit(1);
		}
		if (feof(fp)) break;
	}


	fclose(fp);
}


void doit(int port, char *filespec, char *schemafn, char* bucket, const char* key[], int nkey)
{
	char errmsg[200];
	char* reply = s3pool_pull_ex(port, filespec, schemafn, bucket, key, nkey,
								 errmsg, sizeof(errmsg));
	if (!reply) {
		fatal(errmsg);
	}

	char* p = reply;
	while (1) {
		char* q = strchr(p, '\n');
		if (!q || p == q) break;

		*q = 0;
		catfile(p);
		p = q+1;
	}

	free(reply);
}

char *readfile(const char *fname) {
	char * buffer = 0;
	size_t length;
	FILE * f = fopen (fname, "rb");

	if (f) {
		fseek (f, 0, SEEK_END);
		length = ftell (f);
		fseek (f, 0, SEEK_SET);
		buffer = malloc (length);
		if (buffer) {
			if (fread(buffer, 1, length, f) != length) {
				fclose(f);
				free(buffer);
				return 0;
			}
		}
		fclose (f);
	}
	return buffer;
}




int main(int argc, char* argv[])
{
	int opt;
	int port = -1;
	while ((opt = getopt(argc, argv, "p:h")) != -1) {
		switch (opt) {
		case 'p':
			port = atoi(optarg);
			break;
		case 'h':
			usage(argv[0], 0);
			break;
		default:
			usage(argv[0], "Bad command line flag");
			break;
		}
	}

	if (! (0 < port && port <= 65535)) {
		usage(argv[0], "Bad or missing port number");
	}

	if (optind >= argc) {
		usage(argv[0], "Need filespec, schemafn, bucket and key");
	}

	char *filespecfn = argv[optind++];
	if (optind >= argc) {
		usage(argv[0], "Need schemafn, bucket and key");
	}

	char* schemafn = argv[optind++];
	if (optind >= argc) {
		usage(argv[0], "Need bucket and key");
	}

	char* bucket = argv[optind++];
	if (optind >= argc) {
		usage(argv[0], "Need key");
	}

	char *filespec = readfile(filespecfn);
	if (!filespec) {
		fprintf(stderr, "filespec file %s not found", filespecfn);
		return 1;
	}

	const char** key = (const char**) &argv[optind];
	int nkey = argc - optind;

	filespec[strcspn(filespec, "\r\n")] = 0;

	doit(port, filespec, schemafn, bucket, key, nkey);

	free(filespec);

	return 0;
}
