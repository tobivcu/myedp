/**
 * @file 	server.c
 * @brief	Implementation of a metadata server that incorporates inlinededuplication, smart chunk distribution and recipe construction
 * @Author	Xu Min
 */
#include "listener.h"
#include "common.h"
#include "dedupe.h"
#include "gc.h"
#include "gcbs.h"
#include "image.h"
#include "distributor.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>

#define SIZE (FP_SIZE*1024)

typedef struct _thread_argu {
	int connfd;
	int srv_id;
} THREAD_ARGU;

static int srv_cnt=0;
static int method;
static uint64_t total_chunks=0;
static uint64_t total_parities = 0;
static uint64_t max_chunk_id=0;
KCDB* db;
KCDB* sdmap;
KCDB* csmap;
pthread_mutex_t index_lock;
pthread_mutex_t sdmap_lock;
pthread_mutex_t csmap_lock;

/**
 * Pipeline all services, and send results back to client
 * @param ptr	socket handle with client
 * @return 	NULL
 */
void* process(void* ptr){
	THREAD_ARGU ta = *(THREAD_ARGU *)ptr;
	int connfd = ta.connfd;
	int srv_id = ta.srv_id;
	free(ptr);
	printf("\nConnection fd: %d\n",connfd);

	char buf[BUF_SIZE];
	int stat[ARRAY_SIZE];
	memset(stat,0,sizeof(int)*ARRAY_SIZE);
	
	Queue* lq = NewQueue();
    	Queue* cq = NewQueue();
	Queue* iq = LongQueue();
	Queue* dq = NewQueue();

   	Listener* ln = GetListener();
    	Deduper* ctrl = GetDeduper();
	ImageService* is = GetImageService();
	Distributor* dist = GetDistributor();

    	ln->receiveData(connfd, lq,srv_id);
    	ctrl->DedupeData(lq,cq,db,&index_lock,srv_id);
	dist->start(cq,dq,method,sdmap,&sdmap_lock,csmap,&csmap_lock,srv_id);
	is->start(dq,iq,srv_id);

    	Segment* seg = NULL;
    	int w,n=0,r=0,lgid = -1;
	fprintf(stderr,"starting to send response...\n");
    	while((seg = (Segment*)Dequeue(iq)) != NULL){
		total_chunks++;
		if (seg->stid != lgid){
			total_parities += CODE_SIZE;
			lgid = seg->stid;
		}
		if (seg->unique == 1) {
			if (seg->id > max_chunk_id) max_chunk_id = seg->id;

			memcpy(buf+n,seg,SEG_SIZE);
   			n+=SEG_SIZE;
			if(n == BUF_SIZE){
				while(n>0){
					w = write(connfd,buf+BUF_SIZE-n,n);
				//	printf("sending ... %d", w);
					n -= w;		
				}
				r = 0;
			}
		}
		free(seg);
    	}
	while(n>0){
		w = write(connfd,buf+r,n);
	//	printf("sending ... %d", w);
		r += w;
		n -= w;
	}
	ln->stop(srv_id);
    	ctrl->stop(srv_id);
	dist->stop(srv_id);
	is->stop(srv_id);
	close(connfd);
	free(lq);
   	free(cq);
	free(iq);
	free(dq);
	fprintf(stderr,"\nOverall Dedupe Ratio: %.4f\n Data Dedupe Ratio: %.4f\n (%ld,%ld,%ld)\n\n",(float)(total_chunks+total_parities-max_chunk_id)/(total_chunks+total_parities), (float)(total_chunks+total_parities-max_chunk_id)/total_chunks,total_chunks, total_parities,max_chunk_id);

	sync();
	return NULL;
}

void* process1(void* ptr){
	THREAD_ARGU ta = *(THREAD_ARGU *)ptr;
	int connfd = ta.connfd;
	int srv_id = ta.srv_id;
	free(ptr);
	printf("\nConnection fd: %d\n",connfd);
	printf("Doing garbage collection and re-encode...\n");
	char buf[BUF_SIZE];
	int stat[ARRAY_SIZE];
	memset(stat,0,sizeof(int)*ARRAY_SIZE);
	
	Queue* lq = NewQueue();
    	Queue* cq = NewQueue();
	Queue* iq = LongQueue();
	Queue* dq = NewQueue();

   	Listener* ln = GetListener();
    //	Deduper* ctrl = GetDeduper();
	GCor* ctrl = GetGCor();
	ImageService* is = GetImageService();
	Distributor* dist = GetDistributor();

    	ln->receiveData(connfd, lq,srv_id);
   // 	ctrl->DedupeData(lq,cq,db,&index_lock,srv_id);
	ctrl->GCData(lq,cq,db,&index_lock,srv_id);
	dist->start(cq,dq,method,sdmap,&sdmap_lock,csmap,&csmap_lock,srv_id);
	is->start(dq,iq,srv_id);

    	Segment* seg = NULL;
    	int w,n=0,r=0,lgid = -1;
	fprintf(stderr,"starting to send response...\n");
    	while((seg = (Segment*)Dequeue(iq)) != NULL){
		total_chunks++;
		if (seg->stid != lgid){
			total_parities += CODE_SIZE;
			lgid = seg->stid;
		}
		if (seg->unique == 1) {
			if (seg->id > max_chunk_id) max_chunk_id = seg->id;

			memcpy(buf+n,seg,SEG_SIZE);
   			n+=SEG_SIZE;
			if(n == BUF_SIZE){
				while(n>0){
					w = write(connfd,buf+BUF_SIZE-n,n);
				//	printf("sending ... %d", w);
					n -= w;		
				}
				r = 0;
			}
		}
		free(seg);
    	}
	while(n>0){
		w = write(connfd,buf+r,n);
	//	printf("sending ... %d", w);
		r += w;
		n -= w;
	}
	ln->stop(srv_id);
    	ctrl->stop(srv_id);
	dist->stop(srv_id);
	is->stop(srv_id);
	close(connfd);
	free(lq);
   	free(cq);
	free(iq);
	free(dq);
	fprintf(stderr,"\nOverall Dedupe Ratio: %.4f\n Data Dedupe Ratio: %.4f\n (%ld,%ld,%ld)\n\n",(float)(total_chunks+total_parities-max_chunk_id)/(total_chunks+total_parities), (float)(total_chunks+total_parities-max_chunk_id)/total_chunks,total_chunks, total_parities,max_chunk_id);

	sync();
	return NULL;
}


void* process2(void* ptr){
	THREAD_ARGU ta = *(THREAD_ARGU *)ptr;
	int connfd = ta.connfd;
	int srv_id = ta.srv_id;
	free(ptr);
	printf("\nConnection fd: %d\n",connfd);
	printf("Doing garbage collection and re-encode...\n");
	char buf[BUF_SIZE];
	int stat[ARRAY_SIZE];
	memset(stat,0,sizeof(int)*ARRAY_SIZE);
	
	Queue* lq = NewQueue();
    	Queue* cq = NewQueue();
	Queue* iq = LongQueue();
	Queue* dq = NewQueue();

   	Listener* ln = GetListener();
    //	Deduper* ctrl = GetDeduper();
	GCorbs* ctrl = GetGCorbs();
	ImageService* is = GetImageService();
	Distributor* dist = GetDistributor();

    	ln->receiveData(connfd, lq,srv_id);
   // 	ctrl->DedupeData(lq,cq,db,&index_lock,srv_id);
	ctrl->GCbsData(lq,cq,db,&index_lock,srv_id);
	dist->start(cq,dq,method,sdmap,&sdmap_lock,csmap,&csmap_lock,srv_id);
	is->start(dq,iq,srv_id);

    	Segment* seg = NULL;
    	int w,n=0,r=0,lgid = -1;
	fprintf(stderr,"starting to send response...\n");
    	while((seg = (Segment*)Dequeue(iq)) != NULL){
		total_chunks++;
		if (seg->stid != lgid){
			total_parities += CODE_SIZE;
			lgid = seg->stid;
		}
		if (seg->unique == 1) {
			if (seg->id > max_chunk_id) max_chunk_id = seg->id;

			memcpy(buf+n,seg,SEG_SIZE);
   			n+=SEG_SIZE;
			if(n == BUF_SIZE){
				while(n>0){
					w = write(connfd,buf+BUF_SIZE-n,n);
				//	printf("sending ... %d", w);
					n -= w;		
				}
				r = 0;
			}
		}
		free(seg);
    	}
	while(n>0){
		w = write(connfd,buf+r,n);
	//	printf("sending ... %d", w);
		r += w;
		n -= w;
	}
	ln->stop(srv_id);
    	ctrl->stop(srv_id);
	dist->stop(srv_id);
	is->stop(srv_id);
	close(connfd);
	free(lq);
   	free(cq);
	free(iq);
	free(dq);
	fprintf(stderr,"\nOverall Dedupe Ratio: %.4f\n Data Dedupe Ratio: %.4f\n (%ld,%ld,%ld)\n\n",(float)(total_chunks+total_parities-max_chunk_id)/(total_chunks+total_parities), (float)(total_chunks+total_parities-max_chunk_id)/total_chunks,total_chunks, total_parities,max_chunk_id);

	sync();
	return NULL;
}

void intHandler(){
	kcdbsync(db,1,NULL,NULL);
	kcdbclose(db);
	kcdbdel(db);
	kcdbsync(sdmap,1,NULL,NULL);
	kcdbclose(sdmap);
	kcdbdel(sdmap);
	kcdbsync(csmap,1,NULL,NULL);
	kcdbclose(csmap);
	kcdbdel(csmap);

	exit(0);
}

/**
 * Start server service on specified port
 * Server receive segment fingerprints from client
 * Deduplicate based on these fingerprints
 * Make decision on how to distribute the unique segments
 * Inform client of the decision on distribution
 */
int main(int argc, char** argv){

    if(argc != 3){
	printf("Usage: ./server <port> [BASELINE|EDP|CEDP]\n");
	return 0;
    }
	signal(SIGPIPE,SIG_IGN);
	signal(SIGINT,intHandler);
	pthread_t tid;
	int port = atoi(argv[1]);
	if (strcmp(argv[2],"BASELINE") == 0) method = B;
	else if (strcmp(argv[2],"EDP") == 0) method = E;
	else if (strcmp(argv[2],"CEDP") == 0) method = C;
	else if (strcmp(argv[2],"FEDP") == 0) method = FE;
	else if (strcmp(argv[2],"RAD") == 0) method = R;
	else if (strcmp(argv[2],"GCR") == 0) method = B;
	else if (strcmp(argv[2],"GCB") == 0) method = B;
	else {
		fprintf(stderr,"Invalid distribution method\n");
		exit(1);
	}	

	//Load Index Map
	db = kcdbnew();
	kcdbopen(db,"meta/index/index.kch#bnum=1M#msiz=64M",KCOWRITER|KCOCREATE|KCONOLOCK);
	printf("Index DB Path: %s\n",kcdbpath(db));
	//kcdbloadsnap(db,"meta/index");
	//kcdbdumpsnap(db,"meta/index");

	//Load chunk-disk Map
	sdmap = kcdbnew();
	kcdbopen(sdmap,"meta/index/sdmap.kch#bnum=1M#msiz=64M",KCOWRITER|KCOCREATE|KCONOLOCK);
	//kcdbloadsnap(sdmap,"meta/sdmap");
	//kcdbdumpsnap(sdmap,"meta/sdmap");

	//Load chunk-stripe Map
	csmap = kcdbnew();
	kcdbopen(csmap,"meta/index/csmap.kch#bnum=1M#msiz=64M",KCOWRITER|KCOCREATE|KCONOLOCK);
	//kcdbloadsnap(csmap,"meta/csmap");
	//kcdbdumpsnap(csmap, "meta/csmap");

	int listenfd = 0, connfd = 0;

	struct sockaddr_in serv_addr;
	THREAD_ARGU* ta;	

	listenfd=socket(AF_INET,SOCK_STREAM,0);
	printf("socket retrive success\n");

	memset(&serv_addr, '0', sizeof(struct sockaddr_in));

	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	serv_addr.sin_port = htons(port);

	bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));

	if(listen(listenfd,10) == -1){
		printf("Failed to listen.\n");
		return -1;
	}

	while(1){
		connfd = accept(listenfd, (struct sockaddr*)NULL,NULL);
		srv_cnt++;
		ta = (THREAD_ARGU*)malloc(sizeof(THREAD_ARGU));
		ta->connfd = connfd;
		ta->srv_id = srv_cnt;
		if(strcmp(argv[2],"GCR") == 0)
			pthread_create(&tid,NULL,process1,ta);
		else if(strcmp(argv[2],"GCB") == 0)
                    pthread_create(&tid,NULL,process2,ta);
		else	
			pthread_create(&tid,NULL,process,ta);
	}

    	return 0;

}
