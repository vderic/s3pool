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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include "s3pool.h"

static char* mkrequest(int argc, const char** argv, char* errmsg, int errmsgsz)
{
	int len = 4;				/* for [ ] \n \0 */
	int i;
	char* request = 0;

	for (i = 0; i < argc; i++) {
		// for each arg X, we want to make 'X', - quote quote comma space
		// so, reserve extra space for those chars here
		len += strlen(argv[i]) + 4; 
		/*
		if (strchr(argv[i], '\"')) {
			snprintf(errmsg, errmsgsz, "DQUOTE char not allowed");
			goto bailout;
		}
		*/
		if (strchr(argv[i], '\n')) {
			snprintf(errmsg, errmsgsz, "NEWLINE char not allowed");
			goto bailout;
		}
	}
	request = malloc(len);
	if (! request) {
		snprintf(errmsg, errmsgsz, "out of memory");
		goto bailout;
	}

	char* p = request;
	*p++ = '[';
	for (i = 0; i < argc; i++) {
		sprintf(p, "\"%s\"%s", argv[i], i < argc - 1 ? "," : "");
		p += strlen(p);
	}
	*p++ = ']';
	*p++ = '\n';
	*p = 0;						/* NUL */

	//assert((int)strlen(p) + 1 <= len);
	return request;

	bailout:
	if (request) free(request);
	return 0;
}

static int send_request(int sockfd, const char* request,
						char* errmsg, int errmsgsz)
{
	const char* p = request;
	const char* q = request + strlen(request);
	while (p < q) {
		int n = write(sockfd, p, q-p);
		if (n == -1) {
			if (errno == EAGAIN) continue;
			
			snprintf(errmsg, errmsgsz, "s3pool write: %s", strerror(errno));
			return -1;
		}
			
		p += n;
	}
	return 0;
}



static int check_reply(char* reply, char* errmsg, int errmsgsz)
{
	if (strncmp(reply, "OK\n", 3) == 0) {
		return 0;
	}
	
	if (strncmp(reply, "ERROR\n", 6) == 0) {
		snprintf(errmsg, errmsgsz, "%s", reply + 6);
		return -1;
	}

	if (*reply == '\0') {
		snprintf(errmsg, errmsgsz, "empty reply from s3pool");
		return -1;
	}

	snprintf(errmsg, errmsgsz, "bad message from s3pool: %s", reply);
	return -1;
}





static char* chat(int port, const char* request,
				  char* errmsg, int errmsgsz)
{
	int sockfd = -1;
	struct sockaddr_in servaddr;
	char* reply = 0;

	// socket create and verification
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd == -1) {
		snprintf(errmsg, errmsgsz, "s3pool socket: %s", strerror(errno));
		goto bailout;
	}
	memset(&servaddr, 0, sizeof(servaddr));
	
	// assign IP, PORT
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
	servaddr.sin_port = htons(port);

	// connect the client socket to server socket
	if (connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) != 0) {
		snprintf(errmsg, errmsgsz, "s3pool connect: %s", strerror(errno));
		goto bailout;
	}

	// send the request
	if (-1 == send_request(sockfd, request, errmsg, errmsgsz)) {
		goto bailout;
	}

	// read the reply
	int top, max;
	top = max = 0;
	while (1) {

		if (top == max) {
			int newsz = max * 1.5;
			if (newsz == 0) newsz = 1024;
			char* t = realloc(reply, newsz);
			if (!t) {
				snprintf(errmsg, errmsgsz, "s3pool read: reply message too big -- out of memory");
				goto bailout;
			}
			reply = t;
			max = newsz;
		}

		int n = read(sockfd, reply + top, max - top);
		if (n == -1) {
			if (errno == EAGAIN) continue;

			snprintf(errmsg, errmsgsz, "s3pool read: %s", strerror(errno));
			goto bailout;
		}
		top += n;
		if (n == 0) break;
	}
	if (top == max) {
		char* t = realloc(reply, max + 1);
		if (!t) {
			snprintf(errmsg, errmsgsz, "s3pool read: reply message too big -- out of memory");
			goto bailout;
		}
		reply = t;
		max = max + 1;
	}
	reply[top++] = 0;			/* NUL */

	if (top == 1) {
		snprintf(errmsg, errmsgsz, "s3pool read: 0 bytes from server");
		goto bailout;
	}

	close(sockfd);
	sockfd = -1;

	if (-1 == check_reply(reply, errmsg, errmsgsz)) {
		goto bailout;
	}

	/* reply must contain "OK\nPAYLOAD\n" verified by check_reply() above */
	char* aptr = strdup(reply+3); /* skip to PAYLOAD */
	if (!aptr) {
		snprintf(errmsg, errmsgsz, "s3pool: out of memory");
		goto bailout;
	}

	free(reply);
	return aptr;

	bailout:
	if (sockfd >= 0) close(sockfd);
	if (reply) free(reply);
	return 0;
}



/**

   PULL a file from S3 to local disk. 
 
   On success, return the path to the file pulled down from S3. Caller
   must free() the pointer returned. 
 
   On failure, return a NULL ptr.

 */
char* s3pool_pull_ex(int port, const char *filespec, const char *schemafn, const char* bucket,
					 const char* key[], int nkey,
					 char* errmsg, int errmsgsz)
{
	char* request = 0;
	char* reply = 0;
	int fd = -1;
	const char* argv[4+nkey];

	if (! (nkey > 0)) {
		snprintf(errmsg, errmsgsz, "s3pool_pull_ex: nkey must be > 0");
		return 0;
	}

	argv[0] = "PULL";
	argv[1] = filespec;
	argv[2] = schemafn;
	argv[3] = bucket;
	for (int i = 0; i < nkey; i++)
		argv[i+4] = key[i];
	
	request = mkrequest(4+nkey, argv, errmsg, errmsgsz);
	if (!request) {
		goto bailout;
	}

	reply = chat(port, request, errmsg, errmsgsz);
	if (! reply) {
		goto bailout;
	}


	free(request);
	return reply;

	bailout:
	if (fd != -1) close(fd);
	if (request) free(request);
	if (reply) free(reply);
	return 0;
}

char* s3pool_pull(int port, const char *filespec, const char *schemafn, const char* bucket, const char* key,
				  char* errmsg, int errmsgsz)
{
	char* reply = s3pool_pull_ex(port, filespec, schemafn, bucket, &key, 1, errmsg, errmsgsz);
	if (reply) {
		char* term = strchr(reply, '\n');
		if (term) *term = 0;
	}
	return reply;
}



/**
 *  PUSH a file from local disk to S3. Returns 0 on success, -1 otherwise.
 */
int s3pool_push(int port, const char* bucket, const char* key, const char* fpath,
				char* errmsg, int errmsgsz)
{
	char* request = 0;
	char* reply = 0;
	const char* argv[4] = { "PUSH", bucket, key, fpath };

	request = mkrequest(4, argv, errmsg, errmsgsz);
	if (!request) {
		goto bailout;
	}

	reply = chat(port, request, errmsg, errmsgsz);
	if (! reply) {
		goto bailout;
	}

	free(request);
	free(reply);
	return 0;

	bailout:
	if (request) free(request);
	if (reply) free(reply);
	return -1;
}


/**
 *  REFRESH a bucket list. Returns 0 on success, -1 otherwise.
 */
int s3pool_refresh(int port, const char* bucket,
				   char* errmsg, int errmsgsz)
{
	char* request = 0;
	char* reply = 0;
	const char* argv[2] = { "REFRESH", bucket };

	request = mkrequest(2, argv, errmsg, errmsgsz);
	if (!request) {
		goto bailout;
	}

	reply = chat(port, request, errmsg, errmsgsz);
	if (! reply) {
		goto bailout;
	}

	free(request);
	free(reply);
	return 0;

	bailout:
	if (request) free(request);
	if (reply) free(reply);
	return -1;
}

/**

   GLOB file names in a bucket. 
 
   On success, return a buffer containing strings terminated by
   NEWLINE. Each string is a path name in the S3 bucket that matched
   pattern. Caller must free() the buffer returned.
 
   On failure, return a NULL ptr.

*/
char* s3pool_glob(int port, const char* bucket, const char* pattern,
				  char* errmsg, int errmsgsz)
{
	char* request = 0;
	char* reply = 0;
	const char* argv[3] = {"GLOB", bucket, pattern};

	request = mkrequest(3, argv, errmsg, errmsgsz);
	if (!request) {
		goto bailout;
	}
	reply = chat(port, request, errmsg, errmsgsz);
	if (! reply) {
		goto bailout;
	}

	free(request);
	return reply;

	bailout:
	if (request) free(request);
	if (reply) free(reply);
	return 0;
}

