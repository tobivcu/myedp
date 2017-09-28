#ifndef _TRACE_H_
#define _TRACE_H_

#include <queue.h>
#include "common.h"

#define BLOCK_SIZE 4096ULL
#define USING_FILE
#define FILE_SIZE 1952873 //number of blocks of file

typedef struct{
    int fid;
    int offset;
} Block;

typedef struct{
    int type; //0: HP Synthetic Trace; 1: PeerDedupe Trace  
    pthread_mutex_t _lock;
    int _num;
    int _status[NUM_THREADS];
    int _srvID[NUM_THREADS];
    pthread_t _tid[NUM_THREADS];
    int _fd[NUM_THREADS];
    Queue* _oq[NUM_THREADS];
    Segment* _bseg;
#ifdef USING_FILE 
    int (*receiveData)(int port, Queue* iq,int serverID);
#else
    int (*receiveData)(Queue* iq);
#endif

    int (*stop)(int serverID); 
} Listener;

Listener* GetListener();

#endif
