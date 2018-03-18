/**
 * @file 	dedupe.c
 * @brief	Implementation of Deduplication Service
 * @author	Xu Min
 */
#include "dedupe.h"
#include <queue.h>
#include <kclangc.h>

static Deduper deduper;


/**
 * Set database entry
 * @param seg       Segment to process
 */
static inline void setDatabase(Segment * seg) {

   	seg->unique = 1;
    	pthread_mutex_lock(deduper._lock);
    	seg->id = ++deduper._slog->segID;
    	kcdbadd(deduper._db, (char *)seg->fp, FP_SIZE, (char *)&seg->id, sizeof(uint64_t));
    	pthread_mutex_unlock(deduper._lock);
}

/**
 * Implements IndexService->putSegment()
 */
static int putSegment(Segment * seg, int disk) {
	memcpy(deduper._sen[seg->id].fp, seg->fp, FP_SIZE);
	deduper._sen[seg->id].disk = disk;
	return 0;
}


/**
 * Main loop for processing segments
 * @param ptr       useless
 */
static void * process(void * ptr) {
    	int did = *(int*)ptr;
	free(ptr);
    	Segment * seg = NULL;
   	uint64_t size;
    	uint64_t cnt = 0;
	struct timeval a,b,c;

#ifdef DEBUG
	printf("[DEBUG MODE]\n");
#endif
	
	gettimeofday(&a,NULL);
    	while ((seg = (Segment *) Dequeue(deduper._iq[did])) != NULL) {
		pthread_mutex_lock(deduper._lock);
        	void * idptr = kcdbget(deduper._db, (char *)seg->fp, FP_SIZE, &size); 
		pthread_mutex_unlock(deduper._lock);
#ifdef DEDUP
        	if (idptr == NULL) {
#endif
            	setDatabase(seg);
//		printf("chunk is unique...\n, number of unique chunk is %lu\n", cnt);
	    	cnt++; 	
		/*
	    	seg->disk = cnt%10;
	    	seg->pdisk[0] = 10;
	    	seg->pid[0] = --deduper._lslog->segID;
	    	seg->pdisk[1] = 11; 
	    	seg->pid[1] = --deduper._lslog->segID;
		*/	
#ifdef DEDUP
        	} else { 
//		printf("chunk is duplicate...and current chunk's ref is %d\n", deduper._sen[seg->id].ref);
			seg->id = *(uint64_t *)idptr;
            		seg->unique = 0;
			kcfree(idptr);
			idptr = NULL;
			//fprintf(stderr,"Duplicate chunk %ld\n",seg->id);
		}
#endif
//		printf("process chunk %ld\n",cnt);
		deduper._sen[seg->id].ref++;
		seg->en[0].ref = deduper._sen[seg->id].ref;
//		printf("current chunk's ref is %d\n", deduper._sen[seg->id].ref);
		Enqueue(deduper._oq[did],seg);
    	}

    	Enqueue(deduper._oq[did], NULL );
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	fprintf(stderr,"Deduper Time: %ld.%06ld\n",c.tv_sec,c.tv_usec);
    	return NULL ;
}

/**
 * Implement Deduper->DedupeData()
 * @return 	0 on success, -1 on failure
 */
static int DedupeData(Queue *iq, Queue *oq, KCDB* index, pthread_mutex_t* lock, int serverID){

    pthread_mutex_lock(&deduper._tlock);
    fprintf(stderr,"enter deduplicator%d...\n",deduper._num);
    int *i = (int*)malloc(sizeof(int));
    for (*i=0;*i<NUM_THREADS;(*i)++)
	if ( deduper._status[*i] == 0 )
	    break;

    assert(*i < NUM_THREADS);
    deduper._iq[*i] = iq;
    deduper._oq[*i] = oq;
    deduper._srvID[*i] = serverID;

    deduper._status[*i] = 1;
    deduper._num++;
    
    if (deduper._sen == NULL || deduper._db == NULL) {
    //1. Load Segment Metadata
    int fd;
    fd = open("meta/slog", O_RDWR | O_CREAT, 0644);
    if(ftruncate(fd, MAX_ENTRIES(sizeof(SMEntry))) == -1){
	perror("Ftruncate error");
	exit(1);
    }
    deduper._sen = MMAP_FD(fd,MAX_ENTRIES(sizeof(SMEntry)));
    deduper._slog = (SegmentLog *) deduper._sen;
    //deduper._lslog = (SegmentLog*) &deduper._sen[MAX_ENTRIES(1)-1];
    //if (deduper._lslog->segID == 0) deduper._lslog->segID = MAX_ENTRIES(1)-2;
    close(fd);
	
    //2. Deduplication
    /* Read Index Table */
    deduper._db = index;
    deduper._lock = lock;
    //kcdbopen(deduper._db, "-", KCOWRITER | KCOCREATE);
    //kcdbloadsnap(deduper._db, "meta/index");
    }
	

    int ret = pthread_create(&deduper._tid[*i], NULL, process, i);

    pthread_mutex_unlock(&deduper._tlock);

    return ret;
}

/**
 * Implements Deduper->stop()
 * @return 	0 on success, -1 on failure
 */
static int stop(int serverID){
    pthread_mutex_lock(&deduper._tlock);
    int i;
    for (i=0;i<NUM_THREADS;i++)
	if (deduper._srvID[i] == serverID)
	    break;
    assert(i < NUM_THREADS);
    int ret = pthread_join(deduper._tid[i], NULL);
    deduper._status[i] = 0;
    deduper._num--;
    deduper._srvID[i] = -1;
    pthread_mutex_unlock(&deduper._tlock);

    if (deduper._num == 0){
    	if (!kcdbsync(deduper._db, 1,NULL,NULL))
	    fprintf(stderr,"dedup index sync/dump error: %s\n", kcecodename(kcdbecode(deduper._db)));
    	munmap(deduper._sen, MAX_ENTRIES(sizeof(SMEntry)));
	deduper._sen = NULL;
    }
    printf("Deduplicator exit%d...\n",deduper._num);
    return ret;
}


static Deduper deduper = {
    ._num = 0,
    ._status = {0,0,0,0,0,0,0,0,0,0},
    .DedupeData = DedupeData,
    .stop = stop,
    .putSegment = putSegment,
};

Deduper* GetDeduper(){
    return &deduper;
}
