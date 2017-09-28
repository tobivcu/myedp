/**
 * @file	image.c
 * @brief	Image Service Implementation
 * @author	Ng Chun Ho
 */

#include "image.h"

typedef struct _arg {
	int iid;
	int srvID;
} ARG;

static ImageService service;

/**
 * Main loop for processing segments
 * @param ptr		useless
 */
static void * process(void * ptr) {
	ARG* arg = (ARG*)ptr;
	int iid = arg->iid;
	//int srvID = arg->srvID;
	free(ptr);

	printf("Enter image %d\n",iid);
	uint64_t lfid = -1;
	char buf[128];
	Segment * seg;
	struct stat st = {0};
	/*
	sprintf(buf, "image/%d", srvID);
        if(stat(buf,&st) == -1){
           mkdir(buf, 0775);
        }
	*/

	while ((seg = (Segment *) Dequeue(service._iq[iid])) != NULL) {
		//pthread_mutex_lock(&seg->mutex);
		//pthread_mutex_unlock(&seg->mutex);

		if(seg->fid != lfid){
			if (lfid != -1)
			 	close(service._fd[iid]);

			pthread_mutex_lock(&service._lock);
			sprintf(buf, "image/%ld", (service._ins+seg->fid)/256);
			if(stat(buf,&st) == -1){
				mkdir(buf, 0775);
			}
			//pthread_mutex_lock(&service._lock);
			sprintf(buf, "image/%ld/%ld-%u", (service._ins+seg->fid)/256,service._ins+seg->fid,service._ver);
			//pthread_mutex_unlock(&service._lock);
			do{
			if(access(buf,F_OK) != -1) {
				service._fd[iid] = open(buf,O_RDWR|O_APPEND);
			}
			else {
				service._fd[iid] = creat(buf, 0644);
				//pthread_mutex_lock(&service._lock);
				service._flog->id++;
				//pthread_mutex_unlock(&service._lock);
			}
			} while(service._fd[iid] == -1);
			pthread_mutex_unlock(&service._lock);
			lfid = seg->fid;
		}

		pthread_mutex_lock(&service._lock);
		service._en[service._ins+seg->fid].vers[service._ver].size += seg->len;
		service._en[service._ins+seg->fid].vers[service._ver].space += seg->unique ? seg->len : 0;
		service._en[service._ins+seg->fid].vers[service._ver].csize += seg->clen;
		service._en[service._ins+seg->fid].vers[service._ver].cspace += seg->unique ? seg->clen : 0;
		pthread_mutex_unlock(&service._lock);

		if(seg->unique == 1){
			//kcdbadd(service._map,(char*)&seg->id,sizeof(uint64_t),(char*)&seg->disk,sizeof(int));
			Direct* dir = (Direct*)malloc(sizeof(Direct));
			assert (dir != NULL);
			dir->index = seg->offset;
			dir->id = seg->id;
			dir->disk = seg->disk;
			dir->len = seg->len;
			dir->stid = seg->stid;
			assert(write(service._fd[iid], dir, sizeof(Direct)) == sizeof(Direct));
			free(dir);
			Enqueue(service._oq[iid], seg);
		} else {
			/*
			void* dptr = kcdbget(service._map, (char*)&seg->id,sizeof(uint64_t),&size);
			if(dptr != NULL){
				seg->disk = *(uint8_t*)dptr;
				kcfree(dptr);
				dptr = NULL;
			}
			*/
			//fprintf(stderr,"Duplicate chunk %ld of file %d\n",seg->id,seg->fid);
			Direct* dir = (Direct*)malloc(sizeof(Direct));
			assert (dir != NULL);
			dir->index = seg->offset;
			dir->id = seg->id;
			dir->disk = seg->disk;
			dir->len = seg->len;
			dir->stid = seg->stid;
			assert(write(service._fd[iid], dir, sizeof(Direct)) == sizeof(Direct));
			free(dir);
			//free(seg);
			Enqueue(service._oq[iid],seg);
		}

	}
	Enqueue(service._oq[iid], NULL);

	return NULL;
}

/**
 * Implements ImageService->start()
 */
static int start(Queue * iq, Queue * oq, int serverID) {

	int ret, fd;
	pthread_mutex_lock(&service._lock);
	ARG *arg = (ARG*)malloc(sizeof(ARG));
	int i;
	for (i=0;i<NUM_THREADS;i++)
	    if (service._status[i] == 0)
		break;
	assert(i < NUM_THREADS);
	service._srvID[i] = serverID;
	service._iq[i] = iq;
	service._oq[i] = oq;
	service._num++;
	service._status[i] = 1;

	arg->iid = i;
	arg->srvID = serverID;
	printf("IMAGE: %d\n",i);
	if (service._en == NULL ){
	fd = open("meta/ilog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, INST_MAX(sizeof(IMEntry))));
	service._en = MMAP_FD(fd, INST_MAX(sizeof(IMEntry)));
	close(fd);

	fd = open("meta/flog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, sizeof(FLog)));
    	service._flog = MMAP_FD(fd, sizeof(FLog));
    	close(fd);

	service._ins = service._flog->id;
	service._ver = service._en[service._flog->id].versions++;
	service._en[service._flog->id].recent++;
	}

	ret = pthread_create(&service._tid[i], NULL, process, arg);
	pthread_mutex_unlock(&service._lock);
	return ret;
}

/**
 * Implements ImageService->stop()
 */
static int stop(int serverID) {

	pthread_mutex_lock(&service._lock);
	int i;
	printf("IMAGE CHECK: %d, %d\n",service._srvID[0],serverID);
	for (i=0;i<NUM_THREADS;i++)
	    if (service._srvID[i] ==  serverID)
		break;
	assert(i < NUM_THREADS);
	int ret = pthread_join(service._tid[i], NULL);
	close(service._fd[i]);
	service._num--;
	service._status[i] = 0;
	service._srvID[i] = -1;
	pthread_mutex_unlock(&service._lock);
	/*
	kcdbdumpsnap(service._map, "meta/sdmap");
    kcdbclose(service._map);
    kcdbdel(service._map);
	*/
	if (service._num == 0){
	    munmap(service._en, INST_MAX(sizeof(IMEntry)));
	    munmap(service._flog, sizeof(FLog));
	    service._en = NULL;
	    service._flog = NULL;
	}
	printf("Image Service exit%d...\n",service._num);
	return ret;
}

static ImageService service = {
	._num = 0,
	._status = {0,0,0,0,0,0,0,0,0,0},
	.start = start,
	.stop = stop,
};

ImageService* GetImageService() {
	return &service;
}
