#ifndef _CHUNKING_H_
#define _CHUNKING_H_

#include "common.h"
#include <openssl/sha.h>
#include "rabin.h"
#include "fingerprint.h"
#include <errno.h>
#include <string.h>
#include <dirent.h>

typedef struct {
	pthread_t _tid;
	//char* _if;
	//char* _df;
	int _cnt;
	char** _list;
	int _start;
	int _end;
	uint64_t _vsize;
	Queue* _oq;
	//int (*start)(char* _if,char* _df, Queue* oq);
	int (*start)(char** _list, int start, int end, Queue* oq);
	int (*stop)();
} Chunker;

Chunker* GetChunker();

#endif
