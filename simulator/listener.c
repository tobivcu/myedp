#include "common.h"
#include "listener.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>


static Listener listener;

static void* process(void* ptr){
   
	int lid = *(int*)ptr; 
	free(ptr);
    	char recvBuff[BUF_SIZE];
	memset(recvBuff,'0',BUF_SIZE*sizeof(char));
	int i,n,r=0;
	int cnt=0;
	//uint64_t fsize;
	uint64_t next_offset;
	int32_t next_len;
	int32_t next_fid;
	int done=0;
	struct timeval a,b,c;
	
	Segment* seg = NULL;
	gettimeofday(&a,NULL);
//	printf("before while loop");
	while((n = read(listener._fd[lid], recvBuff+r, BUF_SIZE-r)) >= 0){
		//NOTE: checksum the received fingerprints???
		r += n;
		if(r > 0 && (r % SEG_SIZE == 0)){ 
			for(i=0;i<r;i+=SEG_SIZE){
				seg = (Segment*)malloc(sizeof(Segment));
				assert (seg != NULL);
				cnt++;
				memcpy(seg,recvBuff+i,SEG_SIZE);
				//fsize = seg->fsize;
				//fixed transmission error
				if(seg->voffset > 322122547200){
					printf("Trasmission error: offset %ld, len %d\n",seg->voffset, seg->len);
					//seg->voffset = loffset+llen;
					//seg->len = (seg->fsize-seg->voffset > AVG_SEG_SIZE)?AVG_SEG_SIZE:seg->fsize-seg->voffset;
					seg->voffset = next_offset;
					seg->len = next_len;
				}
				if (seg->fid > 9999999){
					printf("Trasmission error: fid %ld\n",seg->fid);
					//if(seg->offset == 0) seg->fid = lfid +1;
					//else seg->fid = lfid;
					seg->fid = next_fid;
				}
				next_offset = seg->next_voffset;
				next_fid = seg->next_fid;
				next_len = seg->next_len;
				//if(seg->fsize <= seg->voffset+seg->len){
				if (next_fid == -1 && next_len == -1){
					fprintf(stderr,"%ld, %ld, %d\n",seg->fsize,seg->voffset,next_fid);
					done = 1;
				}
			
				Enqueue(listener._oq[lid],seg);
				//printf("Segment of file %d at offset %ld with len %d\n",seg->fid,seg->offset, seg->len);
			}
			r=0;
		}
		//if(strncmp("END",recvBuff+r-4,4) == 0){
		//if(fsize/AVG_SEG_SIZE <= cnt){
		if(done == 1){
			fprintf(stderr,"Request read done\n");
			break;
		}
		
    }
	if(r > 0){
		for(i=0;i<r-4;i+=SEG_SIZE){
		     Segment* seg = (Segment*)malloc(sizeof(Segment));
			 assert (seg != NULL);
			 cnt++;
		     memcpy(seg,recvBuff+i,SEG_SIZE);
			 //printf("Recv segment: %d with %d chunks, position: %ld, len: %d\n",cnt,seg->chunks,seg->offset, seg->len);
		     Enqueue(listener._oq[lid],seg);
		}
	}
	if (n == -1) {
	    fprintf(stderr,"Listener%d FD: %d\n",lid,listener._fd[lid]);
	    perror("Listener...");
 	}
    	Enqueue(listener._oq[lid],NULL);
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	fprintf(stderr,"Listener time: %ld.%06ld, malloc %d segments\n",c.tv_sec,c.tv_usec,cnt);
	return NULL;
}


#ifdef USING_FILE
static int receiveData(int fd, Queue* oq, int serverID){

    pthread_mutex_lock(&listener._lock); 
    int ret;
    int* i = (int*)malloc(sizeof(int));
    for (*i=0;*i<NUM_THREADS;(*i)++)
	if (listener._status[*i] == 0)
	    break;

    assert(*i<NUM_THREADS);
    listener._fd[*i] = fd;
    listener._oq[*i] = oq;
    listener._srvID[*i] = serverID;
    fprintf(stderr,"enter listener%d,%d by server %d...\n",listener._num,*i,serverID);
    ret = pthread_create(&listener._tid[*i],NULL,process,i);

    listener._status[*i] = 1;
    listener._num++;
    pthread_mutex_unlock(&listener._lock);

    return ret;
}
#else
static void receiveData(Queue* oq){

}
#endif

/*
 * @param lid	listener ID
 */
static int stop(int serverID){

    pthread_mutex_lock(&listener._lock);
    int i;
    for (i=0;i<NUM_THREADS;i++)
	if (listener._srvID[i] == serverID)
	    break;

    assert(i < NUM_THREADS);
    int ret = pthread_join(listener._tid[i], NULL);
    
    listener._status[i] = 0;
    listener._num--;
    listener._srvID[i] = -1;
    pthread_mutex_unlock(&listener._lock);
    printf("Listener exit%d,%d...\n",listener._num,i);
    return ret;
}

static Listener listener = {
	._num = 0,
	._status = {0,0,0,0,0,0,0,0,0,0},
	.receiveData = receiveData,
	.stop = stop,
};

Listener* GetListener() {
    return &listener;
}
