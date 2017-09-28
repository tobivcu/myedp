#include <stdio.h>
#include <stdlib.h>
#include "bucket.h"

#define CACHE_LIMIT 1024ULL

typedef struct QNode {
	struct QNode *prev, *next;
	uint64_t id;
	struct timeval t;
	void* data;
} QNode;

typedef struct Cache {
	uint32_t count;
	uint32_t size;
	QNode *front, *rear;
} Cache;

QNode* newQNode( uint64_t id, void* data, uint32_t size, struct timeval t) {
	QNode* tmp = (QNode*)malloc(sizeof(QNode));
	tmp->id = id;
	tmp->data = (char*)malloc(sizeof(char)*size);
	timersub(&(tmp->t),&(tmp->t),&(tmp->t));
	timeradd(&(tmp->t),&t,&(tmp->t));
	memcpy(tmp->data,data,size);

	tmp->prev = tmp->next = NULL;

	return tmp;
}

Cache* createCache( uint32_t size ) {
	Cache* c = (Cache*)malloc(sizeof(Cache));
	c->count = 0;
	c->front = c->rear = NULL;
	c->size = size;

	return c;
}

int isCacheFull(Cache* c) {
	return c->count == c->size;
}

int isCacheEmpty(Cache* c) {
	return c->rear == NULL;
}

void evictCache (Cache* c) {
	if (isCacheEmpty(c)) return;

	if (c->front == c->rear){
		c->front = NULL;
	}

	QNode* tmp = c->rear;
	c->rear = c->rear->prev;

	if (c->rear) c->rear->next = NULL;

	free(tmp->data);
	free(tmp);

	c->count--;
}

void addCache(Cache* c, uint64_t id, void* data, uint32_t size, struct timeval t) {
	if ( isCacheFull(c) ) {
		evictCache(c);
	}

	QNode* tmp = newQNode(id,data,size,t);
	tmp->next = c->front;
	
	if (isCacheEmpty(c))
		c->rear = c->front = tmp;
	else {
		c->front->prev = tmp;
		c->front = tmp;
	}

	c->count++;
}
/**
 * @return 	 address of data on hit, NULL on miss
 */
void* searchCache (Cache* c, uint64_t id, struct timeval t) {
	QNode* tmp;
	for (tmp=c->front;tmp!=NULL;tmp=tmp->next) {
		if (tmp->id == id){
			if (timercmp(&(tmp->t),&t,!=)) {
				//fprintf(stderr,"Bucket %ld: cache time %ld.%06ld, disk time: %ld.%06ld\n",id,tmp->t.tv_sec,tmp->t.tv_usec, t.tv_sec, t.tv_usec);
				if (tmp->prev) tmp->prev->next = tmp->next;
				if (tmp->next) tmp->next->prev = tmp->prev;
				if (tmp == c->rear) {
					c->rear = tmp->prev;
					if (c->rear) c->rear->next = NULL;
				}
				if (tmp == c->front) {
					c->front = tmp->next;
					if (c->front) c->front->prev = NULL;
				}

				free(tmp->data);
				free(tmp);
				c->count--;

				return NULL;
			} else {
			if (tmp != c->front) {
				tmp->prev->next = tmp->next;
				if (tmp->next) tmp->next->prev = tmp->prev;
				if (tmp == c->rear) {
					c->rear = tmp->prev;
					c->rear->next = NULL;
				}

				tmp->next = c->front;
				tmp->prev = NULL;
				tmp->next->prev = tmp;
				c->front = tmp;
			}
			return tmp->data;
			}
		}
	}
	return NULL;
}

void cleanCache(Cache* c) {
	while (!isCacheEmpty(c)) evictCache(c);
}
