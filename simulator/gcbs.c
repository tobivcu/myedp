/**
 * @file 	gc.c
 * @brief	Implementation of gc Service and re-encode
 * @author
 */
#include "gcbs.h"
#include <queue.h>
#include <kclangc.h>

//static Deduper deduper;
static GCorbs gcorbs;

/**
 * Set database entry
 * @param seg       Segment to process
 */
static inline void setDatabase(Segment * seg) {

   	seg->unique = 1;
    	pthread_mutex_lock(gcorbs._lock);
    	seg->id = ++gcorbs._slog->segID;
    	kcdbadd(gcorbs._db, (char *)seg->fp, FP_SIZE, (char *)&seg->id, sizeof(uint64_t));
    	pthread_mutex_unlock(gcorbs._lock);
}

/**
 * Implements IndexService->putSegment()
 */
static int putSegment(Segment * seg, int disk) {
	memcpy(gcorbs._sen[seg->id].fp, seg->fp, FP_SIZE);
	gcorbs._sen[seg->id].disk = disk;
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
    	while ((seg = (Segment *) Dequeue(gcorbs._iq[did])) != NULL) {
//originally, if database NULL means it's new chunk, in GC which will not happen, so commet out.
//		pthread_mutex_lock(gcor._lock);
//       	void * idptr = kcdbget(gcor._db, (char *)seg->fp, FP_SIZE, &size); 
//		pthread_mutex_unlock(gcor._lock);
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
//			seg->id = *(uint64_t *)idptr;
//           		seg->unique = 0;
//			kcfree(idptr);
//			idptr = NULL;
			//fprintf(stderr,"Duplicate chunk %ld\n",seg->id);
//		}
//#endif
//		printf("process chunk %ld\n",cnt);
		
		
       	        printf("current chunk's ref is %d\n", gcorbs._sen[seg->id].ref);
               	Enqueue(gcorbs._oq[did],seg);
    	
}
    	Enqueue(gcorbs._oq[did], NULL );
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	fprintf(stderr,"GCor Time: %ld.%06ld\n",c.tv_sec,c.tv_usec);
    	return NULL ;
}

/**
 * Implement Deduper->DedupeData()
 * @return 	0 on success, -1 on failure
 */
static int GCbsData(Queue *iq, Queue *oq, KCDB* index, pthread_mutex_t* lock, int serverID){

    pthread_mutex_lock(&gcorbs._tlock);
    fprintf(stderr,"enter garbage collector%d...\n",gcorbs._num);
    int *i = (int*)malloc(sizeof(int));
    for (*i=0;*i<NUM_THREADS;(*i)++)
	if ( gcorbs._status[*i] == 0 )
	    break;

    assert(*i < NUM_THREADS);
    gcorbs._iq[*i] = iq;
    gcorbs._oq[*i] = oq;
    gcorbs._srvID[*i] = serverID;

    gcorbs._status[*i] = 1;
    gcorbs._num++;
    
    if (gcorbs._sen == NULL || gcorbs._db == NULL) {
    //1. Load Segment Metadata
    int fd;
    fd = open("meta/slog", O_RDWR | O_CREAT, 0644);
    if(ftruncate(fd, MAX_ENTRIES(sizeof(SMEntry))) == -1){
	perror("Ftruncate error");
	exit(1);
    }
    gcorbs._sen = MMAP_FD(fd,MAX_ENTRIES(sizeof(SMEntry)));
    gcorbs._slog = (SegmentLog *) gcorbs._sen;
    //deduper._lslog = (SegmentLog*) &deduper._sen[MAX_ENTRIES(1)-1];
    //if (deduper._lslog->segID == 0) deduper._lslog->segID = MAX_ENTRIES(1)-2;
    close(fd);
	
    //2. Deduplication
    /* Read Index Table */
    gcorbs._db = index;
    gcorbs._lock = lock;
    //kcdbopen(deduper._db, "-", KCOWRITER | KCOCREATE);
    //kcdbloadsnap(deduper._db, "meta/index");
    }
	

    int ret = pthread_create(&gcorbs._tid[*i], NULL, process, i);

    pthread_mutex_unlock(&gcorbs._tlock);

    return ret;
}

/**
 * Implements Deduper->stop()
 * @return 	0 on success, -1 on failure
 */
static int stop(int serverID){
    pthread_mutex_lock(&gcorbs._tlock);
    int i;
    for (i=0;i<NUM_THREADS;i++)
	if (gcorbs._srvID[i] == serverID)
	    break;
    assert(i < NUM_THREADS);
    int ret = pthread_join(gcorbs._tid[i], NULL);
    gcorbs._status[i] = 0;
    gcorbs._num--;
    gcorbs._srvID[i] = -1;
    pthread_mutex_unlock(&gcorbs._tlock);

    if (gcorbs._num == 0){
    	if (!kcdbsync(gcorbs._db, 1,NULL,NULL))
	    fprintf(stderr,"gc index sync/dump error: %s\n", kcecodename(kcdbecode(gcorbs._db)));
    	munmap(gcorbs._sen, MAX_ENTRIES(sizeof(SMEntry)));
	gcorbs._sen = NULL;
    }
    printf("Garbage collector exit%d...\n",gcorbs._num);
    return ret;
}


static GCorbs gcorbs = {
    ._num = 0,
    ._status = {0,0,0,0,0,0,0,0,0,0},
    .GCbsData = GCbsData,
    .stop = stop,
    .putSegment = putSegment,
};

GCorbs* GetGCorbs(){
    return &gcorbs;
}
