/**
 * @file	distributor.h
 * @brief	Distribution Service Definition
 * @author	Xu Min
 */
#ifndef _DISTRIBUTOR_H_
#define _DISTRIBUTOR_H_
#include "common.h"
#include <queue.h>
#include <kclangc.h>
#include "segment.h"
#include "EDP/dedup.h"
#include "EDP/parity_declustering.h"

typedef struct _FList {
	struct	_FList* next;
	FEntry en;
	int segs;
} FList;

typedef struct{
	int _num;
	pthread_mutex_t _lock;
	int _status[NUM_THREADS];
	int _srvID[NUM_THREADS];
	pthread_t _tid[NUM_THREADS];
	Queue* _iq[NUM_THREADS];
	Queue* _oq[NUM_THREADS];
	int stat[NUM_THREADS][ARRAY_SIZE];
	int* _f[NUM_THREADS];
	int _fSize[NUM_THREADS];
	int* _DArray[NUM_THREADS];
	int* _PDArray[NUM_THREADS];
	int* _CDArray[NUM_THREADS];
	int* _CPDArray[NUM_THREADS];
	struct ReferIntraSegment* _RArray[NUM_THREADS];
	int _sizeofR[NUM_THREADS];
	SegBuf* _sb[NUM_THREADS];
	uint8_t* _bitmap[NUM_THREADS];
	
	KCDB* _map;
 	pthread_mutex_t* _map_lock;
	KCDB* _csmap;
	pthread_mutex_t* _csmap_lock;
	STEntry* _sten;
	StripeLog* _stlog;
	SMEntry* _sen;
	SegmentLog* _slog;
	int _method;
	int _cost[ARRAY_SIZE];
	int (*start)(Queue* iq, Queue* oq, int method, KCDB* sdmap, pthread_mutex_t* sdmap_lock, KCDB* csmap, pthread_mutex_t* csmap_lock, int serverID);
	int (*stop)(int serverID);
} Distributor;

Distributor* GetDistributor();
#endif
