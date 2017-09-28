/**
 * @file	bucket.h
 * @brief	Bucket Service Header File
 * @author 	Xu Min
 */
#ifndef BUCKET_H_
#define BUCKET_H_

#include "common.h"

#define BUCKET_SIZE (MAX_SEG_SIZE*64)

typedef struct {
	uint64_t id;
	uint64_t offset;
	uint32_t size;
	void* data;
} Bucket;

typedef struct {
	uint64_t id;
	uint32_t size;
	struct timeval t;
} BMEntry;

typedef struct {
	uint64_t id;
} BucketLog;

static Bucket* NewBucket(BMEntry* en, BucketLog* bl) {
	Bucket* b = malloc(sizeof(Bucket));
	if (bl->id > 0 && (en[bl->id].size <= BUCKET_SIZE-MAX_SEG_SIZE)) {
	b->id = bl->id;
	b->offset = (bl->id-1)*BUCKET_SIZE+en[bl->id].size;
	b->size = 0;
	b->data = malloc(sizeof(char)*(BUCKET_SIZE-b->size));
	memset(b->data,0,sizeof(char)*(BUCKET_SIZE-b->size));
	} else {
	b->id = ++bl->id;
	b->offset = (b->id-1)*BUCKET_SIZE;
	b->size = 0;
	b->data = malloc(sizeof(char)*BUCKET_SIZE);
	memset(b->data,0,sizeof(char)*BUCKET_SIZE);
	}
	return b;
}

static void SaveBucket(BMEntry* en, Bucket** b, int fd) {
	//Write In-memory bucket data to disk
	lseek(fd,(*b)->offset,SEEK_SET);
	assert(write(fd,(*b)->data,(*b)->size) == (*b)->size);
	//fprintf(stderr,"Write %d bytes to bucket %ld\n",(*b)->size,(*b)->id);
	//Update bucket metadata
	en[(*b)->id].size += (*b)->size;
	gettimeofday(&(en[(*b)->id].t),NULL);

	free((*b)->data);
	free((*b));
	*b = NULL;
}

static Bucket* InsertBucket(Bucket* b, void* data, int len, int fd, BMEntry* en, BucketLog* bl) {
	if ( b == NULL) b = NewBucket(en,bl);

	//printf("Bucket size %d(%d), %d\n",b->size,en[b->id].size,len);
	if (en[b->id].size + b->size + len > BUCKET_SIZE) {
		SaveBucket(en,&b,fd);
		b = NewBucket(en,bl);
	}

	memcpy(b->data+b->size, data, len);
	b->size += len;

	return b;
}

#endif
