/**
 * @file 	gc.c
 * @brief	Implementation of gc Service and re-encode
 * @author
 */
#include "gc.h"
#include <queue.h>
#include <kclangc.h>

//static Deduper deduper;
static GCor gcor;

/**
 * Set database entry
 * @param seg       Segment to process
 */
static inline void setDatabase(Segment * seg) {

   	seg->unique = 1;
    	pthread_mutex_lock(gcor._lock);
    	seg->id = ++gcor._slog->segID;
    	kcdbadd(gcor._db, (char *)seg->fp, FP_SIZE, (char *)&seg->id, sizeof(uint64_t));
    	pthread_mutex_unlock(gcor._lock);
}

/**
 * Implements IndexService->putSegment()
 */
static int putSegment(Segment * seg, int disk) {
	memcpy(gcor._sen[seg->id].fp, seg->fp, FP_SIZE);
	gcor._sen[seg->id].disk = disk;
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
	int i = 0;
    	uint64_t cnt1 = 0;
	uint64_t cnt2 = 0;
	uint64_t rem1 = 0;
	uint64_t rem2 = 0;
	uint64_t sum = 0;
	struct timeval a,b,c;

#ifdef DEBUG
	printf("[DEBUG MODE]\n");
#endif
	
	gettimeofday(&a,NULL);
    	while ((seg = (Segment *) Dequeue(gcor._iq[did])) != NULL) {
//originally, if database NULL means it's new chunk, in GC which will not happen, so commet out.
		pthread_mutex_lock(gcor._lock);
	       	void * idptr = kcdbget(gcor._db, (char *)seg->fp, FP_SIZE, &size); 
		pthread_mutex_unlock(gcor._lock);
//#ifdef DEDUP
//        	if (idptr == NULL) {
//#endif
//            	setDatabase(seg);
//		printf("chunk is unique...\n, number of unique chunk is %lu\n", cnt);
//	    	cnt++; 	
		/*
	    	seg->disk = cnt%10;
	    	seg->pdisk[0] = 10;
	    	seg->pid[0] = --deduper._lslog->segID;
	    	seg->pdisk[1] = 11; 
	    	seg->pid[1] = --deduper._lslog->segID;
		*/	
//#ifdef DEDUP
//        	} else { 
//		printf("chunk is duplicate...and current chunk's ref is %d\n", deduper._sen[seg->id].ref);
			seg->id = *(uint64_t *)idptr;
           		seg->unique = 0;
			kcfree(idptr);
			idptr = NULL;
			if(gcor._sen[seg->id].ref == 1)
				cnt1++;
			else
				cnt2++;
			//fprintf(stderr,"Duplicate chunk %ld\n",seg->id);
//		}
//#endif
//		printf("process chunk %ld\n",cnt);
	    	}
	rem1 = cnt1%12;
	rem2 = cnt2%12;
	sum = (12 - rem1) + (12 - rem2);
	
	for(i=0; i<sum; i++){
		Enqueue(gcor._oq[did], seg);
	}

    	Enqueue(gcor._oq[did], NULL );
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	fprintf(stderr,"GCor Time: %ld.%06ld\n",c.tv_sec,c.tv_usec);
    	return NULL ;
}

/**
 * Implement Deduper->DedupeData()
 * @return 	0 on success, -1 on failure
 */
static int GCData(Queue *iq, Queue *oq, KCDB* index, pthread_mutex_t* lock, int serverID){

    pthread_mutex_lock(&gcor._tlock);
    fprintf(stderr,"enter garbage collector%d...\n",gcor._num);
    int *i = (int*)malloc(sizeof(int));
    for (*i=0;*i<NUM_THREADS;(*i)++)
	if ( gcor._status[*i] == 0 )
	    break;

    assert(*i < NUM_THREADS);
    gcor._iq[*i] = iq;
    gcor._oq[*i] = oq;
    gcor._srvID[*i] = serverID;

    gcor._status[*i] = 1;
    gcor._num++;
    
    if (gcor._sen == NULL || gcor._db == NULL) {
    //1. Load Segment Metadata
    int fd;
    fd = open("meta/slog", O_RDWR | O_CREAT, 0644);
    if(ftruncate(fd, MAX_ENTRIES(sizeof(SMEntry))) == -1){
	perror("Ftruncate error");
	exit(1);
    }
    gcor._sen = MMAP_FD(fd,MAX_ENTRIES(sizeof(SMEntry)));
    gcor._slog = (SegmentLog *) gcor._sen;
    //deduper._lslog = (SegmentLog*) &deduper._sen[MAX_ENTRIES(1)-1];
    //if (deduper._lslog->segID == 0) deduper._lslog->segID = MAX_ENTRIES(1)-2;
    close(fd);
	
    //2. Deduplication
    /* Read Index Table */
    gcor._db = index;
    gcor._lock = lock;
    //kcdbopen(deduper._db, "-", KCOWRITER | KCOCREATE);
    //kcdbloadsnap(deduper._db, "meta/index");
    }
	

    int ret = pthread_create(&gcor._tid[*i], NULL, process, i);

    pthread_mutex_unlock(&gcor._tlock);

    return ret;
}

/**
 * Implements Deduper->stop()
 * @return 	0 on success, -1 on failure
 */
static int stop(int serverID){
    pthread_mutex_lock(&gcor._tlock);
    int i;
    for (i=0;i<NUM_THREADS;i++)
	if (gcor._srvID[i] == serverID)
	    break;
    assert(i < NUM_THREADS);
    int ret = pthread_join(gcor._tid[i], NULL);
    gcor._status[i] = 0;
    gcor._num--;
    gcor._srvID[i] = -1;
    pthread_mutex_unlock(&gcor._tlock);

    if (gcor._num == 0){
    	if (!kcdbsync(gcor._db, 1,NULL,NULL))
	    fprintf(stderr,"gc index sync/dump error: %s\n", kcecodename(kcdbecode(gcor._db)));
    	munmap(gcor._sen, MAX_ENTRIES(sizeof(SMEntry)));
	gcor._sen = NULL;
    }
    printf("Garbage collector exit%d...\n",gcor._num);
    return ret;
}


static GCor gcor = {
    ._num = 0,
    ._status = {0,0,0,0,0,0,0,0,0,0},
    .GCData = GCData,
    .stop = stop,
    .putSegment = putSegment,
};

GCor* GetGCor(){
    return &gcor;
}
