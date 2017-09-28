/**
 * @file	dedupe.h
 * @brief	Deduplication Service Definition
 * @author 	Xu Min
 */
#ifndef _DEDUPE_H_
#define _DEDUPE_H_

#include "common.h"
#include <queue.h>
#include <kclangc.h>
#include "bloom.h"

typedef struct{
    //Individual info
    int _num;
    pthread_mutex_t _tlock;
    int _status[NUM_THREADS];
    int _srvID[NUM_THREADS];
    pthread_t _tid[NUM_THREADS];
    Queue* _iq[NUM_THREADS];
    Queue* _oq[NUM_THREADS];

    //Shared metadata
    SMEntry* _sen;
    SegmentLog* _slog;
    SegmentLog* _lslog;
    KCDB* _db;
    pthread_mutex_t* _lock;

    FEntry* _fen;
    FLog* _flog;

    int (*DedupeData)(Queue *iq, Queue *oq, KCDB* index, pthread_mutex_t* lock, int serverID);
    int (*stop)(int serverID);
    int (*putSegment)(Segment*seg, int disk);
} Deduper;

Deduper* GetDeduper();
#endif
