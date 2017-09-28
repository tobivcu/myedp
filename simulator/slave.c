#include "common.h"
#include "bucket.h"
#include "lru.h"
#include <kclangc.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <assert.h>



//TO DO: Incorporate encoder/decoder here
//1) Add two threads for communication between slaves
//2) Add strip metadata management
//3) Decoding

char* wd;
int chunk_store; //large flat file to store all unique chunks for the slave; to retrieve a specific chunk, query the map with chunk id as key and offset in the file returned as value.
int cnt;
pthread_mutex_t map_lock;
KCDB* map;
char* path;
BMEntry* en;
BucketLog* bl;
Cache* bc;
/*
 * Each receive process responsible for one segment of data
 * , and store the segment in file named of segment id
 */
void* process(void* ptr){
	int connfd = *(int *)ptr;
	struct timeval a,b,c,d;
	Bucket* bkt = NULL;
	timersub(&d,&d,&d);
	char hd[SEG_SIZE];
	char buf[MAX_SEG_SIZE];
	int kcerr,r,n=0;

	pthread_mutex_lock(&map_lock);
	if(map == NULL){
		memset(path,'0',sizeof(char)*64);
		sprintf(path,"meta/%s_index.kch#bnum=100M#msiz=1G",wd);
		printf("create index file at %s\n",path);
		map = kcdbnew();
		kcdbopen(map,path,KCOWRITER|KCOCREATE|KCONOLOCK);
		//kcdbloadsnap(map,path);
	}
	pthread_mutex_unlock(&map_lock);

	while(1){
	n=0;
	while((r = read(connfd,hd+n,sizeof(uint64_t)+sizeof(uint32_t)-n))> 0){
		n += r;
	}
	if ( r < 0 ) perror("Header read");
	if(n > 0){
	//Segment seg;
	uint64_t sig = 0;
	uint64_t size;
	uint64_t sid;
	uint32_t slen;
	memcpy(&sid,hd,sizeof(uint64_t));
	memcpy(&slen,hd+sizeof(uint64_t),sizeof(uint32_t));

	//fetch current size of stored chunks

	pthread_mutex_lock(&map_lock);
	void* ofptr = kcdbget(map,(char*)&sig,sizeof(uint64_t),&size);
	pthread_mutex_unlock(&map_lock);
	if (ofptr == NULL)
		fprintf(stderr,"Fetch file size failure: %s\n",kcecodename(kcdbecode(map)));
	uint64_t offset = *(uint64_t*)ofptr;
	kcfree(ofptr);
	ofptr = NULL;

	/*
	sprintf(path,"%s/%08lx",wd,seg.id);
	printf("Write segment %ld to file %s\n",seg.id,path);
	int fd = creat(path,0644);
	assert(fd != -1);
	*/

	n = 0;
	while((r=read(connfd,buf+n,slen-n))>0){
		n+= r;
	}
	if ( r < 0) perror("Chunk Read");
	gettimeofday(&a,NULL);
	//lseek(chunk_store,offset,SEEK_SET);
	//write(chunk_store,buf,slen);
	bkt = InsertBucket(bkt,buf,slen,chunk_store, en, bl);
	if (bkt != NULL) fprintf(stderr,"Bucket %ld: Insert chunk %ld bytes to bucket of size %d(%d)\n", bkt->id,sid,bkt->size,en[bkt->id].size);
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	timeradd(&c,&d,&d);

	//add new (sig,offset) entry to map
	offset = (bkt->id-1)*BUCKET_SIZE+en[bkt->id].size+bkt->size-slen;
	pthread_mutex_lock(&map_lock);
	kcerr = kcdbadd(map,(char*)&(sid),sizeof(uint64_t),(char*)&offset,sizeof(uint64_t));
	pthread_mutex_unlock(&map_lock);
	if (kcerr < 0 ) fprintf(stderr,"Add chunk fail: %s\n",kcecodename(kcdbecode(map)));
	offset += slen;
	//update current chunk size
	pthread_mutex_lock(&map_lock);
	kcerr = kcdbreplace(map,(char*)&sig,sizeof(uint64_t),(char*)&offset,sizeof(uint64_t));
	pthread_mutex_unlock(&map_lock);
	if (kcerr < 0 ) fprintf(stderr,"Update size fail: %s\n",kcecodename(kcdbecode(map)));
	}
	if( n <= 0 && r <= 0)
		break;
	}
	pthread_mutex_lock(&map_lock);
	if(map != NULL){
		kcdbsync(map,1,NULL,NULL);
		//kcdbdumpsnap(map, path);
		//kcdbclose(map);
		//kcdbdel(map);
		//map = NULL;
		printf("sync kcdb...\n");
	}
	pthread_mutex_unlock(&map_lock);
	if (bkt != NULL) SaveBucket(en,&bkt,chunk_store);
	printf("Seek time %ld.%06ld\n",d.tv_sec,d.tv_usec);
  	close(connfd);
	sync();
	return NULL;

}

void* process2(void* ptr){
	int connfd = *(int *)ptr;
	char hd[768];
	char buf[MAX_SEG_SIZE];
	char btmp[BUCKET_SIZE];
	int i,r,w,n=0,wn=0;
	uint64_t size;
	struct timeval a,b,c,d;
  d.tv_sec = 0;
  d.tv_usec = 0;

	sprintf(path,"log/%s_read_log",wd);
  /*
	FILE* lfd=fopen(path,"a");
	assert(lfd != NULL);
	fprintf(lfd,"\n*******************************************\n");
	*/
	pthread_mutex_lock(&map_lock);
	if(map == NULL){
		memset(path,'0',sizeof(char)*64);
		sprintf(path,"meta/%s_index.kch#bnum=100M#msiz=1G",wd);
		//printf("[HEHE]create index file at %s\n",path);
		while( (map = kcdbnew()) == NULL) ;
		kcdbopen(map,path,KCOWRITER|KCOCREATE|KCONOLOCK);
		//kcdbloadsnap(map,path);
	}
	pthread_mutex_unlock(&map_lock);


	while(1){
		n=0;
		while((r = read(connfd,hd+n,sizeof(uint32_t)-n))> 0){
			n += r;
		}
		if (r<0) perror("[READER]Num Read");
		if(n > 0){
	    gettimeofday(&a,NULL);
			uint64_t sid;
			uint32_t len;
      uint32_t num;
      memcpy(&num,hd,sizeof(uint32_t));
      //fprintf(stderr,"%d chunks to download\n",num);
      for (i=0;i<num;i++) {
        n=0;
		    while((r = read(connfd,hd+n,sizeof(uint64_t)+sizeof(uint32_t)-n))> 0){
			     n += r;
		    }
			if (r < 0) perror("[READER]header read");
			  memcpy(&sid,hd,sizeof(uint64_t));
			  memcpy(&len,hd+sizeof(uint64_t),sizeof(uint32_t));

			pthread_mutex_lock(&map_lock);
			void* ofptr = kcdbget(map,(char*)&sid,sizeof(uint64_t),&size);
			pthread_mutex_unlock(&map_lock);
			if(ofptr != NULL){
			uint64_t offset = *(uint64_t*)ofptr;
			kcfree(ofptr);
			ofptr = NULL;

			void* data = searchCache(bc,offset/BUCKET_SIZE+1,en[offset/BUCKET_SIZE+1].t);
			//fprintf(stderr,"Chunk %ld  in bucket %llu(%ld/%llu)",sid,offset/BUCKET_SIZE+1,offset,BUCKET_SIZE);
			if (data != NULL) {
				memcpy(buf,data+offset%BUCKET_SIZE,len);
				//fprintf(stderr,", [HIT]\n");
			}
			else {
				lseek(chunk_store,(offset/BUCKET_SIZE)*BUCKET_SIZE,SEEK_SET);
				assert(read(chunk_store,btmp,BUCKET_SIZE) == BUCKET_SIZE);
				addCache(bc,offset/BUCKET_SIZE+1,btmp,BUCKET_SIZE,en[offset/BUCKET_SIZE+1].t);
				memcpy(buf,btmp+offset%BUCKET_SIZE,len);
				//fprintf(stderr,",[MISS]\n");
			}
			} else {
      fprintf(stderr,"[READER] Chunk Lookup Fail: %s(%ld)\n",kcecodename(kcdbecode(map)),sid);
      }
			wn = 0;
			while((w=write(connfd,buf+wn,len-wn))>0)
					wn += w;
			//close(fd);
      }
	    gettimeofday(&b,NULL);
	    timersub(&b,&a,&c);
      timeradd(&c,&d,&d);
    }
		if(n <= 0 && r<= 0){
			perror("Socket Read Error");
			break;
		}
	}
	/*
	if(map != NULL){
		//kcdbdumpsnap(map, path);
		kcdbclose(map);
		kcdbdel(map);
		map = NULL;
		printf("close kcdb...\n");
	}
	*/
	close(connfd);
	//fprintf(lfd,"Read time: %ld.%06ld\n",d.tv_sec,d.tv_usec);
	//fclose(lfd);
	//kcdbdumpsnap(map, path);
	//kcdbclose(map);
	//kcdbdel(map);

	return NULL;
}

void* worker(void* ptr){
	int port = *(int*)ptr;
	pthread_t tid;
	int listenfd = 0, connfd = 0;
	struct sockaddr_in slave_addr;
	memset(&slave_addr,'0',sizeof(struct sockaddr_in));
	slave_addr.sin_family = AF_INET;
	slave_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	slave_addr.sin_port = htons(port);


	listenfd = socket(AF_INET,SOCK_STREAM,0);
	/*
	 * set socket address reusable
	 */
	int on = 1, status=0;
	status = setsockopt(listenfd,SOL_SOCKET,SO_REUSEADDR,(const char *)&on, sizeof(on));
	if (status == -1)
		perror ("setsockopt(...,SO_REUSEADDR,...)");
	/*
	 * set socket linger to ensure complete transmission after socket close
	 */
	{
		struct linger linger = {0};
		linger.l_onoff = 1;
		linger.l_linger = 30;
		status = setsockopt(listenfd,SOL_SOCKET,SO_LINGER,(const char *)&linger, sizeof(linger));
		if (status == -1)
			perror("setsockopt(...,SO_LINGER,...)");
	}

	bind(listenfd, (struct sockaddr*)&slave_addr, sizeof(slave_addr));

	if(listen(listenfd,10) == -1){
		printf("Failed to listen.\n");
		return NULL;
	}

	while(1){
		connfd = accept(listenfd,(struct sockaddr*)NULL,NULL);
		if(port/10000 == 1){
			pthread_join(tid,NULL);
			pthread_create(&tid,NULL,process,&connfd);
		}
		else{
			cnt++;
			pthread_join(tid,NULL);
			perror("Download request");
			pthread_create(&tid,NULL,process2,&connfd);
		}
	}
}

void intHandler(){
	if(map != NULL){
		kcdbsync(map,1,NULL,NULL);
		kcdbdumpsnap(map, path);
		kcdbclose(map);
		kcdbdel(map);
		map = NULL;
		printf("close kcdb...\n");
	}
	free(path);
	munmap(en, MAX_ENTRIES(sizeof(BMEntry)));
	close(chunk_store);
	printf("close chunk_store...\n");
	cleanCache(bc);
	exit(0);
}

int main(int argc, char** argv){
	if (argc != 4){
		printf("Usage: ./slave <upload_port> <restore_port> <directory>\n");
		exit(1);
	}

	signal(SIGINT, intHandler);

	int port = atoi(argv[1]);  //range: 1111x
	int port2 = atoi(argv[2]); //range: 2222x


	wd = argv[3];
	cnt = 0;
	path = (char*)malloc(sizeof(char)*64);
	sprintf(path,"%s/chunk_store",wd);
	chunk_store = open(path,O_RDWR);
	assert(chunk_store != -1);
    memset(path,'0',sizeof(char)*64);

	//Load Bucket Metadata
	sprintf(path,"meta/%s_blog",wd);
	int fd = open(path, O_RDWR|O_CREAT, 0644);
	assert(!ftruncate(fd,MAX_ENTRIES(sizeof(BMEntry))/16));
	en = MMAP_FD(fd,MAX_ENTRIES(sizeof(BMEntry)));
	bl = (BucketLog*) en;
	close(fd);
	bc = createCache(CACHE_LIMIT);

	sprintf(path,"meta/%s_index.kch#bnum=100M#msiz=1G",wd);
	//Check if index exists
	if(open(path,O_RDONLY) == -1){
		printf("Create index file at %s\n",path);
		map = kcdbnew();
		kcdbopen(map,path,KCOWRITER|KCOCREATE|KCONOLOCK);
		//kcdbloadsnap(map,path);
		uint64_t slog =0;
		uint64_t offset = 0;
		kcdbadd(map,(char*)&slog,sizeof(uint64_t),(char*)&offset,sizeof(uint64_t));
	}

	if(map != NULL){
		kcdbsync(map,1,NULL,NULL);
		//kcdbdumpsnap(map, path);
		//kcdbclose(map);
		//kcdbdel(map);
		//map = NULL;
		//printf("close kcdb...\n");
	}

	pthread_t tid,tid2;
	pthread_create(&tid,NULL,worker,&port);
	pthread_create(&tid2,NULL,worker,&port2);

  	//fprintf(stderr,"TID: %d, TID2: %d\n", tid, tid2);
	pthread_join(tid,NULL);
	pthread_join(tid2,NULL);

	return 0;
}
