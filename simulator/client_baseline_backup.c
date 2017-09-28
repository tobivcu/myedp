/**
 * \file 	client.c
 * \brief	A client that initiates upload/download request with metadata server and transfer raw data between storage slaves
 * Author 	Xu Min
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include "chunking.h"
#include "rpc/llist.h"
#include "lib/jerasure/jerasure.h"
#include "lib/jerasure/reed_sol.h"

#define DISK_NUM ARRAY_SIZE
#define CACHE_LIMIT 1024
#define ENCODER_NUM 1
#define DECODER_NUM 1
char* host;

pthread_spinlock_t file_lock;
pthread_mutex_t file_mutex;
int *matrix;
int failed[ARRAY_SIZE];
int reader_synced;
pthread_mutex_t reader_sync_mutex;
pthread_cond_t reader_sync_cond;
pthread_mutex_t decode_lock[DECODER_NUM];
pthread_cond_t decode_cond[DECODER_NUM];
int decoding[DECODER_NUM];
pthread_mutex_t encode_lock;
pthread_mutex_t dmatrix_lock;
#ifdef CHECK_READ
char* orig_file;
#endif

#ifdef FSL
char dummy[16384];
#endif
/**
 * \struct Argument that is passed to each data transfer thread
 */
typedef struct _pArgs{
	uint64_t id;
	void** data;
	void* fd;
	uint64_t* dcnt;
	Queue* q;
	Queue** oq;
	pthread_spinlock_t lock;
} pArgs;

/**
 * \struct Queue entry telling download thread on information of required chunks
 */
typedef struct _QEntry{
	uint64_t id;
	uint64_t offset;
	uint8_t disk;
	uint32_t len;				/*!< Length of read data */
	uint32_t dlen;              /*!< Length of reconstructed data */
	uint8_t unique;
	uint8_t degraded; 			/*!<0: Normal Read; 1: Degraded-Read */
	uint8_t target; 			/*!<Index of strip that is re-constructed */
	uint8_t index; 				/*!<Index in the stripe */
	uint8_t stlen; 				/*!<Number of data blocks in the stripe */
	uint8_t dec_id;				/*!<ID of decoder */
	void* data;
} QEntry;

/**
 * \struct Structure of cached chunk of data
 */
typedef struct _cachelist{
	uint64_t id;
	uint32_t len;
	uint16_t ref;
	void* data;
} CacheList;


void* encoder(void* ptr)
{
	pArgs pa = *(pArgs*)ptr;
	uint64_t fsize = pa.id;
	Segment* seg = NULL;
	Segment* stripe[STRIPE_SIZE];
	char **data, **coding;
	struct timeval a, b, c;
	long elapsed;
	double ectp, elapsed_double, fsize_double;
	//char *dtmp;
	//int pdisk[CODE_SIZE];
	int lgid = -1, st_cnt=0;
	int i,stlen;
	int k = STRIPE_SIZE-CODE_SIZE,
		m = CODE_SIZE,
		w = 8;
	
//	printf("File size from encoder is %ld bytes\n",fsize);
	data = (char**)malloc(sizeof(char*)*k);
	coding = (char**)malloc(sizeof(char*)*m);
	//dtmp = (char*)malloc(sizeof(char)*MAX_SEG_SIZE);
	//memset(dtmp,0,sizeof(char)*MAX_SEG_SIZE);
	gettimeofday(&a, NULL);
	while ((seg = Dequeue(pa.q)) != NULL) {
		//fprintf(stderr,"%d-th chunk: %d, last_gid: %d\n",st_cnt,seg->stid,lgid);
		if (seg->stid != lgid) {
			//First Stripe Starts
			if (lgid != -1) {
				//encoding last stripe of data
				for (i=0;i<m;i++)
					coding[i] = (char*)malloc(sizeof(char)*MAX_SEG_SIZE);
				//Padding Empty Strips
				for (i=stlen;i<k;i++){
					data[i] = (char*)malloc(sizeof(char)*MAX_SEG_SIZE);
					memset(data[i],0,sizeof(char)*MAX_SEG_SIZE);
				}
				pthread_mutex_lock(&encode_lock);
				jerasure_matrix_encode(k,m,w,matrix,data,coding,MAX_SEG_SIZE);
				pthread_mutex_unlock(&encode_lock);
				//printf("Encoding Complete:\n\n");
				//print_data_and_coding(st_cnt,m,w,MAX_SEG_SIZE,data,coding);
				for (i=0;i<m;i++) {
					stripe[st_cnt] = (Segment*)malloc(sizeof(Segment));
					stripe[st_cnt]->id = stripe[0]->pid[i];
					stripe[st_cnt]->len = MAX_SEG_SIZE;
					stripe[st_cnt]->data = coding[i];
					stripe[st_cnt]->stid = stripe[0]->stid; //or, lgid
					stripe[st_cnt]->disk = stripe[0]->pdisk[i];
					stripe[st_cnt]->stlen = stlen;
					//fprintf(stderr,"code chunk %ld to slave %d\n",stripe[st_cnt]->id,stripe[st_cnt]->disk);
					st_cnt++;
				}

				for (i=stlen;i<k;i++)
					free(data[i]);

				//Dispatch encoded data to writer
				//fprintf(stderr,"STRIPE:");
				for (i=0;i<st_cnt;i++){
					//fprintf(stderr,"chunk %ld to slave %d,",stripe[i]->id,stripe[i]->disk);
					Enqueue(pa.oq[stripe[i]->disk],stripe[i]);
				}
				//fprintf(stderr,"\n");

			}
			st_cnt = 0;
			stlen = 0;
			data[st_cnt] = (char*)malloc(sizeof(char)*MAX_SEG_SIZE);
			memset(data[st_cnt],0,sizeof(char)*MAX_SEG_SIZE);
			stripe[st_cnt] = (Segment*)malloc(sizeof(Segment));
			memcpy(stripe[st_cnt],seg,sizeof(Segment));
#ifndef FSL
			memcpy(data[st_cnt],pa.data[seg->fid]+seg->offset,seg->len);
#else
			memcpy(data[st_cnt],dummy,seg->len);
#endif
			stripe[st_cnt]->data = data[st_cnt];
			//fprintf(stderr,"data chunk %ld to slave %d\n",stripe[st_cnt]->id,stripe[st_cnt]->disk);
			st_cnt++;
			stlen++;
			lgid = seg->stid;
		} else {
			data[st_cnt] = (char*)malloc(sizeof(char)*MAX_SEG_SIZE);
			memset(data[st_cnt],0,sizeof(char)*MAX_SEG_SIZE);
            stripe[st_cnt] = (Segment*)malloc(sizeof(Segment));
			memcpy(stripe[st_cnt],seg,sizeof(Segment));
#ifndef FSL
			memcpy(data[st_cnt],pa.data[seg->fid]+seg->offset,seg->len);
#else
			memcpy(data[st_cnt],dummy,seg->len);
#endif
			stripe[st_cnt]->data = data[st_cnt];
			//fprintf(stderr,"data chunk %ld to slave %d\n",stripe[st_cnt]->id,stripe[st_cnt]->disk);
			st_cnt++;
			stlen++;
		}
		free(seg);
	}
	if (st_cnt > 0) {
		//encoding last stripe of data
		for (i=0;i<m;i++)
			coding[i] = (char*)malloc(sizeof(char)*MAX_SEG_SIZE);
		//stlen = stripe[0]->stlen;
		//fprintf(stderr,"Generating %d,%d matrix, %d blocks\n",st_cnt,m,stlen);
		//Padding Empty Strips
		for (i=stlen;i<k;i++){
			data[i] = (char*)malloc(sizeof(char)*MAX_SEG_SIZE);
			memset(data[i],0,sizeof(char)*MAX_SEG_SIZE);
		}
		jerasure_matrix_encode(k,m,w,matrix,data,coding,MAX_SEG_SIZE);
		for (i=0;i<m;i++) {
			stripe[st_cnt] = (Segment*)malloc(sizeof(Segment));
			stripe[st_cnt]->id = stripe[0]->pid[i];
			stripe[st_cnt]->len = MAX_SEG_SIZE;
			stripe[st_cnt]->data = coding[i];
			stripe[st_cnt]->stid = stripe[0]->stid; //or, lgid
			stripe[st_cnt]->disk = stripe[0]->pdisk[i];
			stripe[st_cnt]->stlen = stlen;
			st_cnt++;
		}
		for (i=stlen;i<k;i++)
			free(data[i]);

		/*
		Test encoding correctness
		int *erasures = malloc(sizeof(int)*(m+1));
		erasures[0] = 0;
		erasures[1] = 4;
		erasures[2] = -1;
		memcpy(dtmp,data[0],MAX_SEG_SIZE);
		memset(data[0],0,MAX_SEG_SIZE);
		memset(coding[0],0,MAX_SEG_SIZE);
		i = jerasure_matrix_decode(k,m,w,matrix,1,erasures,data,coding,MAX_SEG_SIZE);
		if (memcmp(dtmp,data[0],MAX_SEG_SIZE) == 0){
			fprintf(stderr,"encoding correct.\n");
			write(fd,data[0],stripe[0]->len);
		}else
			fprintf(stderr,"encoding wrong.\n");

		*/
		//if (pa.id == 1){
		//int fd = creat("/run/shm/rs_coding.tmp",0644);
		//if (fd == -1) perror("CREATE FILE");
		//ftruncate(fd,st_cnt*MAX_SEG_SIZE);
		//Dispatch encoded data to writer
		for (i=0;i<st_cnt;i++){
			//fprintf(stderr,"%d(%d),",stripe[i]->disk,stripe[i]->len);
			//lseek(fd,i*MAX_SEG_SIZE,SEEK_SET);
			//write(fd,stripe[i]->data,MAX_SEG_SIZE);
			Enqueue(pa.oq[stripe[i]->disk],stripe[i]);
		}
		//close(fd);
		//}
	}
	//free(dtmp);
	free(pa.q);
	gettimeofday(&b, NULL);
	timersub(&b, &a, &c);
	printf("Encoding Time: %ld.%06ld s\n",c.tv_sec,c.tv_usec);
	printf("c.tv_sec is %ld, c.tc_usec is %ld\n",c.tv_sec, c.tv_usec);
	elapsed = (b.tv_sec - a.tv_sec) * 1000000 + ((b.tv_usec - a.tv_usec));
	printf("elapsed time is %ld us\n", elapsed);
	fsize_double = (double) (fsize/1048576.0f);
	elapsed_double = (double) (elapsed/1000000.0f);
	ectp = fsize_double/elapsed_double;
	printf("The file size is %lf MB and elapsed time is %lf s\n", fsize_double, elapsed_double);
	printf("The encoding throughput is %lf MB/s\n", ectp);
	return NULL;
}

void* decoder(void* ptr){

	pArgs pa = *(pArgs*)ptr;
	int fd = *(int*)pa.fd;
	QEntry* l = NULL;
	int *erasures = (int*)malloc(sizeof(int)*(CODE_SIZE+1));
	int	*erased = (int*)malloc(sizeof(int)*STRIPE_SIZE);
	char **data, **coding;
	int dcnt = 0,i,j;
	int k = STRIPE_SIZE-CODE_SIZE,
		m = CODE_SIZE,
		w = 8;
	uint64_t offset,len;
	uint8_t target,stlen;
	//int lstid = -1;
	//int tmpfd = open("/run/shm/rs_coding.tmp",O_RDONLY);
	//char *dtmp = (char*)malloc(sizeof(char)*MAX_SEG_SIZE);

	for (i=0;i<STRIPE_SIZE;i++){
		erased[i] = 1;
	}
	data = (char**)malloc(sizeof(char*)*(STRIPE_SIZE-CODE_SIZE));
	for (i=0;i<(STRIPE_SIZE-CODE_SIZE);i++){
		data[i] = malloc(sizeof(char)*MAX_SEG_SIZE);
		memset(data[i],0,sizeof(char)*MAX_SEG_SIZE);
	}
	coding = (char**)malloc(sizeof(char*)*CODE_SIZE);
	for (i=0;i<CODE_SIZE;i++){
		 coding[i] = malloc(sizeof(char)*MAX_SEG_SIZE);
		 memset(coding[i],0,sizeof(char)*MAX_SEG_SIZE);
	}
	/*
	int tmpfd = creat("/run/shm/rs_coding1.tmp",0644);
	if (tmpfd == -1) perror("CREATE FILE");
	ftruncate(tmpfd,(k+m)*MAX_SEG_SIZE);
	*/
	while((l=Dequeue(pa.q)) != NULL){
		dcnt++;
		erased[l->index] = 0;
		//fprintf(stderr,"Stripe index %d\n",l->index);
		if (l->index < STRIPE_SIZE-CODE_SIZE){
			//data[l->index] = l->data;
			memcpy(data[l->index],l->data,l->len);
			//lseek(tmpfd,l->index*MAX_SEG_SIZE,SEEK_SET);
			//write(tmpfd,l->data,l->len);
		} else {
			//coding[l->index-STRIPE_SIZE+CODE_SIZE] = l->data;
			memcpy(coding[l->index-STRIPE_SIZE+CODE_SIZE],l->data,l->len);
			//lseek(tmpfd,l->index*MAX_SEG_SIZE,SEEK_SET);
			//write(tmpfd,l->data,l->len);
			//fprintf(stderr,"CODING[%d]\n",l->index-STRIPE_SIZE+CODE_SIZE);
		}

		offset = l->offset;
		len = l->dlen;
		target = l->target;
		stlen = l->stlen;

		if (dcnt == MIN(k,stlen)) {
			fprintf(stderr,"Reconstruct %d strip, %d, %d\n",target,dcnt,stlen+CODE_SIZE);
			//Fill required data block with zeros
			if (dcnt < k) {
				for (i=0;i<k-dcnt;i++){
					//data[i+stlen] = (char*)malloc(sizeof(char)*MAX_SEG_SIZE);
					memset(data[i+stlen],0,sizeof(char)*MAX_SEG_SIZE);
					erased[i+stlen] = 0;
				}
			}

			for (i=0,j=0;i<STRIPE_SIZE;i++)
				if (erased[i] == 1) {
					erasures[j] = i;
					j++;
				}
			erasures[j] = -1;

			printf("Erasures(%d):%d,%d,%d\n",j,erasures[0],erasures[1],erasures[2]);
			/*
			int tmpfd = creat("/run/shm/rs_coding2.tmp",0644);
			if (tmpfd == -1) perror("CREATE FILE");
			ftruncate(tmpfd,(k+m)*MAX_SEG_SIZE);
			for (i=0;i<STRIPE_SIZE;i++) {
				//if (erased[i] == 0){
					lseek(tmpfd,i*MAX_SEG_SIZE,SEEK_SET);
					if ( i < k)
						write(tmpfd,data[i],MAX_SEG_SIZE);
					else
						write(tmpfd,coding[i-STRIPE_SIZE+CODE_SIZE],MAX_SEG_SIZE);
				//}
			}
			close(tmpfd);
			*/

			pthread_mutex_lock(&dmatrix_lock);
			i = jerasure_matrix_decode(k,m,w,matrix,1,erasures,data,coding,MAX_SEG_SIZE);
			pthread_mutex_unlock(&dmatrix_lock);
			pthread_mutex_lock(&file_mutex);
#ifdef SEEK_ON_WRITE
			assert(lseek(fd,offset,SEEK_SET)==offset);
#endif
			assert(write(fd,data[target],len) == len);
			pthread_mutex_unlock(&file_mutex);
				//}
			//}

			for (i=0;i<STRIPE_SIZE;i++) erased[i] = 1;
			dcnt = 0;
		}

		free(l->data);
		free(l);
		l->data = NULL;
		l = NULL;
	}

	//close(tmpfd);
	free(erasures);
	free(erased);
	//free(dtmp);
	for (i=0;i<(STRIPE_SIZE-CODE_SIZE);i++)
		free(data[i]);
    for (i=0;i<CODE_SIZE;i++)
        free(coding[i]);
	free(data);
	free(coding);
	free(pa.q);
	//close(tmpfd);
	return NULL;
}

/**
 * Transfer stream of chunks of data to corresponding slaves
 * @param 	ptr	instance of pArgs
 * @return 	NULL
 */
void* writer(void* ptr)
{
	pArgs pa = *(pArgs*)ptr;
	Segment* seg=NULL;
	struct timeval a,b,c,d;
	//Connect with slave
	int connfd = 0,
		cnt = 0,
		cur_len = 0,
		i;
	uint32_t slen[64];

	char *buf= (char*)malloc(sizeof(char)*MAX_SEG_SIZE*64);
	char *mbuf = (char*)malloc(sizeof(char)*SEG_SIZE*64);

#ifndef SIMULATION
	struct sockaddr_in slave_addr;
	memset(&slave_addr,'0',sizeof(slave_addr));
	slave_addr.sin_family = AF_INET;

	switch(pa.id/2){
	case 0: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.17");break;
	case 1: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.18");break;
	case 2: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.19");break;
	case 3: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.20");break;
	case 4: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.21");break;
	case 5: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.22");break;
	case 6: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.23");break;
	case 7: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.24");break;
	case 8: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.25");break;
	default: fprintf(stderr,"Invalid Slave ID.\n");
	}

	//slave_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	slave_addr.sin_port = htons(11110+pa.id);

	connfd = socket(AF_INET,SOCK_STREAM,0);
		if(connect(connfd,(struct sockaddr*) &slave_addr,sizeof(struct sockaddr)) < 0){
			fprintf(stderr,"ID: %ld\n",pa.id);
			perror("Connection Failure");
			exit(1);
		}
#endif
	gettimeofday(&d,NULL);
	timersub(&d,&d,&d);
	while((seg = Dequeue(pa.q)) != NULL){
		cnt++;

#ifndef SIMULATION
		memcpy(mbuf+(cnt-1)%64*(sizeof(uint64_t)+sizeof(uint32_t)),&(seg->id),sizeof(uint64_t));
		memcpy(mbuf+(cnt-1)%64*(sizeof(uint64_t)+sizeof(uint32_t))+sizeof(uint64_t),&(seg->len),sizeof(uint32_t));
		slen[(cnt-1)%64]=seg->len;
		//uint64_t fid = seg->fid;
		//memcpy(buf+cur_len,pa.data[fid]+seg->offset,seg->len);
		memcpy(buf+cur_len,seg->data,seg->len);
		cur_len += seg->len;
		//fprintf(stderr,"Write chunk %ld to slave %d\n",seg->id,pa.id);

		if (cnt%64 ==0) {
			gettimeofday(&a,NULL);
			cur_len = 0;
			for (i=0;i<64;i++) {
				int w,n=0;
				while((w=write(connfd,mbuf+(sizeof(uint64_t)+sizeof(uint32_t))*i+n,sizeof(uint64_t)+sizeof(uint32_t)-n))>0) n+=w;

				n=0;
				while ((w=write(connfd,buf+cur_len+n,slen[i]-n))>0) {
					n+=w;
				}
				cur_len += slen[i];

			}
			cur_len=0;
			gettimeofday(&b,NULL);
		timersub(&b,&a,&c);
		timeradd(&c,&d,&d);
		}
#endif
		free(seg->data);
		free(seg);
	}
#ifndef SIMULTAION
		if (cnt%64 != 0) {
			gettimeofday(&a,NULL);
			cur_len = 0;
			for (i=0;i<(cnt%64);i++) {
				int w,n=0;
				while((w=write(connfd,mbuf+(sizeof(uint64_t)+sizeof(uint32_t))*i+n,sizeof(uint64_t)+sizeof(uint32_t)-n))>0) n+=w;

				n=0;
				while ((w=write(connfd,buf+cur_len+n,slen[i]-n))>0) {
					n+=w;
				}
				cur_len += slen[i];

			}
			cur_len=0;
			gettimeofday(&b,NULL);
	        timersub(&b,&a,&c);
	        timeradd(&c,&d,&d);
		}
#endif
	printf("Close connection %ld.%06ld\n",d.tv_sec,d.tv_usec);
#ifndef SIMULATION
	close(connfd);
#endif

	free(buf);
	free(mbuf);

	free(pa.q);

	return NULL;
}

/**
 * Download required chunks of data from corresponding slaves
 * @param 	ptr	instance of pArgs
 * @return 	NULL
 */
void* reader(void* ptr)
{
	pArgs pa = *(pArgs*)ptr;
	int cnt=0;
	uint64_t tlen = 0;
	QEntry* l=NULL;
	struct timeval d;
#ifndef SIMULATION
	CacheList cl[CACHE_LIMIT];
	int cllen = 0;
	int slen[64];
	uint64_t soff[64];
	uint64_t sid[64];
	QEntry *qen[64];
	struct sockaddr_in slave_addr;
	memset(&slave_addr,'0',sizeof(slave_addr));
	slave_addr.sin_family = AF_INET;


	//char* dtmp=(char*)malloc(sizeof(char)*MAX_SEG_SIZE);
	//int tmpfd = creat("/run/shm/chunk5.tmp",0644);
	switch(pa.id/2){
	case 0: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.17");break;
	case 1: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.18");break;
	case 2: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.19");break;
	case 3: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.20");break;
	case 4: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.21");break;
	case 5: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.22");break;
	case 6: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.23");break;
	case 7: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.24");break;
	case 8: slave_addr.sin_addr.s_addr = inet_addr("10.10.10.25");break;
	default: fprintf(stderr,"Invalid Slave ID.\n");
	}
	//slave_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	slave_addr.sin_port = htons(21110+pa.id);

	connfd = socket(AF_INET,SOCK_STREAM,0);
		if(connect(connfd,(struct sockaddr*) &slave_addr,sizeof(struct sockaddr)) < 0){
			fprintf(stderr,"\n Error : Connect Slave %ld Failed\n", pa.id);
			failed[pa.id] = 1;
			pthread_mutex_lock(&reader_sync_mutex);
			reader_synced++;
			if (reader_synced == ARRAY_SIZE) pthread_cond_signal(&reader_sync_cond);
			pthread_mutex_unlock(&reader_sync_mutex);
			//perror("Slave connection");
			return NULL;
		} else {
			failed[pa.id] = 0;
			pthread_mutex_lock(&reader_sync_mutex);
			reader_synced++;
			if (reader_synced == ARRAY_SIZE) pthread_cond_signal(&reader_sync_cond);
			pthread_mutex_unlock(&reader_sync_mutex);

		}
	gettimeofday(&d,NULL);
	timersub(&d,&d,&d);
	int wsize = 64;

#endif


	while((l=Dequeue(pa.q)) != NULL){
		tlen += l->len;
#ifndef SIMULATION
		if (l->unique == 1){
			cnt++;
			gettimeofday(&a,NULL);
			memcpy(mbuf+sizeof(uint32_t)+((cnt-1)%wsize)*(sizeof(uint64_t)+sizeof(uint32_t)),&l->id,sizeof(uint64_t));
			memcpy(mbuf+sizeof(uint32_t)+((cnt-1)%wsize)*(sizeof(uint64_t)+sizeof(uint32_t))+sizeof(uint64_t),&l->len,sizeof(uint32_t));
			slen[((cnt-1)%wsize)] = l->len;
			soff[((cnt-1)%wsize)] = l->offset;
			sid[((cnt-1)%wsize)] = l->id;
			qen[((cnt-1)%wsize)] = l;
			int32_t w,n;

			if (cnt%wsize == 0) {
				memcpy(mbuf,&wsize,sizeof(uint32_t));
				n=0;
				while((w=write(connfd,mbuf+n,sizeof(uint32_t)+wsize*(sizeof(uint64_t)+sizeof(uint32_t))-n))>0){
		    		n+=w;
				}

				for (i=0;i<wsize;i++) {
					n=0;
					//how to maintain order of writes among multiple readers???
					while((w=read(connfd,buf+n,slen[i]-n)) > 0){
						n+=w;
					}
					//Update Cache List
					//fprintf(stderr,"Downloaded %d,%d bytes\n",slen[i],n);
					if (cllen < CACHE_LIMIT){
						cl[cllen].id = sid[i];
						cl[cllen].len = slen[i];
						cl[cllen].ref = 1;
						cl[cllen].data = (char*)malloc(sizeof(char)*slen[i]);
						memcpy(cl[cllen].data,buf,slen[i]);
						cllen++;
					} else {
						cl[CACHE_LIMIT-1].id = sid[i];
						cl[CACHE_LIMIT-1].len = slen[i];
						cl[CACHE_LIMIT-1].ref = 1;
						free(cl[CACHE_LIMIT-1].data);
						cl[CACHE_LIMIT-1].data = (char*)malloc(sizeof(char)*slen[i]);
						memcpy(cl[CACHE_LIMIT-1].data,buf,slen[i]);
					}
					//For normal read, write downloaded chunk of data to file
					if (qen[i]->degraded == 0){
						////Seek-on-write
						pthread_mutex_lock(&file_mutex);
#ifdef SEEK_ON_WRITE
						assert(lseek(fd,soff[i],SEEK_SET)==soff[i]);
#endif
						assert(write(fd,buf,n) == n);
#ifdef CHECK_READ
						memcpy(cmp_data,orig_file+soff[i],n);
						if (memcmp(cmp_data,buf,n) != 0)
							fprintf(stderr,"Chunk %ld at %ld on disk %d error\n",qen[i]->id, soff[i],pa.id);
#endif
						pthread_mutex_unlock(&file_mutex);
						free(qen[i]);
						qen[i] = NULL;
					} else {
						//For degraded read, enqueue downloaded chunk to decoder
						qen[i]->data = (void*)malloc(sizeof(char)*MAX_SEG_SIZE);
						memset(qen[i]->data,0,sizeof(char)*MAX_SEG_SIZE);
						memcpy(qen[i]->data,buf,n);
						//fprintf(stderr,"download chunk %ld for degraded read\n",qen[i]->id);
						//if (pa.id == 2) {
						//	write(tmpfd,buf,n);
						//}
						pthread_mutex_lock(&(decode_lock[qen[i]->dec_id]));
						decoding[qen[i]->dec_id]--;
						//printf("Decoding %ld: %d\n",qen[i]->id,qen[i]->len);
						if (decoding[qen[i]->dec_id] == 0) pthread_cond_signal(&(decode_cond[qen[i]->dec_id]));
						pthread_mutex_unlock(&(decode_lock[qen[i]->dec_id]));
						Enqueue(pa.oq[qen[i]->dec_id],qen[i]);
					}
				}
			}
			gettimeofday(&b,NULL);
			timersub(&b,&a,&c);
			timeradd(&c,&d,&d);
		} else {
			//Fetch data from cache
			//NOTICE: Modify the logic so that, if data has been evicted from
			//		  cache, download it again from slave
			for (i=0;i<cllen;i++) {
				if (cl[i].id == l->id) {
					assert(l->len == cl[i].len);
					memcpy(buf,cl[i].data,l->len);
					//For normal read, write downloaded chunk of data to file
					if (l->degraded == 0){
					pthread_mutex_lock(&file_mutex);
#ifdef SEEK_ON_WRITE
					assert(lseek(fd,l->offset,SEEK_SET)==l->offset);
#endif
					assert(write(fd,buf,l->len) == l->len);
					pthread_mutex_unlock(&file_mutex);
					free(l);
					l = NULL;
					} else {
						//For degraded read, enqueue downloaded chunk to decoder
						l->data = (void*)malloc(sizeof(char)*MAX_SEG_SIZE);
						memset(l->data,0,sizeof(char)*MAX_SEG_SIZE);
						memcpy(l->data,buf,l->len);

						pthread_mutex_lock(&(decode_lock[l->dec_id]));
						decoding[l->dec_id]--;
						//printf("Decoding %ld: %d\n",l->id,l->len);
						if (decoding[l->dec_id] == 0) pthread_cond_signal(&(decode_cond[l->dec_id]));
						pthread_mutex_unlock(&(decode_lock[l->dec_id]));
						Enqueue(pa.oq[l->dec_id],l);
					}
					(cl[i].ref)++;
					break;
				}
			}
			//Ascend the cache list by reference count
			for (;i>0;i--) {
				if (cl[i].ref > cl[i-1].ref) {
					uint16_t rtmp = cl[i].ref;
					uint64_t idtmp = cl[i].id;
					uint32_t lentmp = cl[i].len;
					void* dtmp = cl[i].data;
					cl[i].ref = cl[i-1].ref;
					cl[i].id = cl[i-1].id;
					cl[i].len = cl[i-1].len;
					cl[i].data = cl[i-1].data;
					cl[i-1].ref = rtmp;
					cl[i-1].id = idtmp;
					cl[i-1].len = lentmp;
					cl[i-1].data = dtmp;
				} else {
					break;
				}
			}

		}
#else
		if (l->unique == 1)
			cnt++;
#endif
	}
#ifndef SIMULATION
	gettimeofday(&a,NULL);
	if (cnt%wsize != 0) {
		uint32_t w,n=0;
		w = cnt%wsize;
		memcpy(mbuf,&w,sizeof(uint32_t));
		while((w=write(connfd,mbuf+n,sizeof(uint32_t)+(cnt%wsize)*(sizeof(uint64_t)+sizeof(uint32_t))-n))>0){
		    n+=w;
		}
		for (i=0;i<(cnt%wsize);i++) {
			n=0;
			while((w=read(connfd,buf+n,slen[i]-n)) > 0){
				n+=w;
			}
			if (qen[i]->degraded == 0){
			pthread_mutex_lock(&file_mutex);
#ifdef SEEK_ON_WRITE
			assert(lseek(fd,soff[i],SEEK_SET)==soff[i]);
#endif
			assert(write(fd,buf,n) == n);
			pthread_mutex_unlock(&file_mutex);
			free(qen[i]);
			qen[i]=NULL;
			} else {
					//For degraded read, enqueue downloaded chunk to decoder
					qen[i]->data = (void*)malloc(sizeof(char)*MAX_SEG_SIZE);
					memset(qen[i]->data,0,sizeof(char)*MAX_SEG_SIZE);
					memcpy(qen[i]->data,buf,n);
					//fprintf(stderr,"TO DECODER\n");
					pthread_mutex_lock(&(decode_lock[qen[i]->dec_id]));
					decoding[qen[i]->dec_id]--;
					//printf("decoding of %d: %d\n",qen[i]->dec_id,decoding[qen[i]->dec_id]);
					if (decoding[qen[i]->dec_id] == 0) pthread_cond_signal(&(decode_cond[qen[i]->dec_id]));
					pthread_mutex_unlock(&(decode_lock[qen[i]->dec_id]));
					Enqueue(pa.oq[qen[i]->dec_id],qen[i]);
			}

		}
	}
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	timeradd(&c,&d,&d);
#endif

	printf("Node %ld. %d segments of size %ld %ld.%06ld\n",pa.id,cnt,tlen,d.tv_sec,d.tv_usec);
	free(pa.q);
	//free(dtmp);
	//close(tmpfd);
#ifndef SIMULATION
	close(connfd);
	for (i=0;i<cllen;i++) free(cl[i].data);
#endif
	return NULL;
}


/**
 * Traverse files in a given directory
 * @param 	path	path of the directory
 * @param	list	list of files that are in the directory
 * @param	num	number of files inside the directory
 * @param	prefix	current path
 */
void recur_file_list(char* path, char** list, int* num, char* prefix)
{
	DIR* idir = opendir(path);
	char _prefix[256];

	if(!idir){
		fprintf(stderr,"Cannot open directory %s: %s\n",path,strerror(errno));
        exit(1);
	}
	struct dirent* direntp;
	chdir(path);

	while((direntp=readdir(idir))!=NULL){
		if(!(strcmp(direntp->d_name,"."))||!(strcmp(direntp->d_name,"..")))
			continue;
		else if(direntp->d_type & DT_DIR){
			memset(_prefix,'0',sizeof(char)*256);
			strcpy(_prefix, prefix);
			strcat(_prefix,direntp->d_name);
			strcat(_prefix,"/");
			recur_file_list(direntp->d_name,list,num,_prefix);
			chdir("..");
		} else {
			struct stat file_info;
			if (lstat(direntp->d_name,&file_info) == -1)
				perror("STAT");
			if (S_ISREG(file_info.st_mode)){
			int rfd = open(direntp->d_name,O_RDONLY);
			uint64_t sz = lseek(rfd,0,SEEK_END);
			if(sz >= 16384){
			(*num)++;
			//*list = (char**)realloc(*list,sizeof(char*)*(*num));
			(list)[(*num)-1] = (char*)malloc(sizeof(char)*256);
			memset((list)[(*num)-1],'\0',sizeof(char)*256);
			memcpy((list)[(*num)-1],prefix,strlen(prefix));
			memcpy((list)[(*num)-1]+strlen(prefix), direntp->d_name, strlen(direntp->d_name));
			}
			close(rfd);
			//printf("Entry No. %d: %ld\n",*num, sz);
			}
		}

	}

	closedir(idir);
}

/**
 * Send upload request, consisting of chunk metadata, to metadata server
 * \param	ptr	instance of pArgs
 * \return 	NULL
 */
void* request(void* ptr)
{
	pArgs args = *(pArgs*)ptr;
	uint64_t fsize = args.id;
	int connfd = *(int*)(args.fd);
	Queue* cq = args.q;
	char* buf=(char*)malloc(sizeof(char)*BUF_SIZE);

	Segment* seg = NULL;
	Segment* next = NULL;
	int w,n=0,r = 0;
	int cnt=0;
	//Send Upload Request with fingerprints of segment
	seg = Dequeue(cq);
	printf("File size %lu\n",fsize);
	while((next = Dequeue(cq)) != NULL){
		cnt++;
		seg->fsize = fsize;

		seg->next_offset = next->offset;
		seg->next_len = next->len;
		seg->next_fid = next->fid;
		seg->next_voffset = next->voffset;
		memcpy(buf+r,seg,SEG_SIZE);
		//fprintf(stderr,"chunk of file %d at %ld with length %d\n",seg->fid, seg->offset,seg->len);
		free(seg);

		seg = next;

		r+=SEG_SIZE;

		if(r == BUF_SIZE){
			n = 0;
			while( (w = write(connfd,buf+n,r-n)) > 0)
				n+=w;

			if ( w == -1) {
				perror("Write request");
				exit(1);
			}
			r = 0;
		}

	}
	seg->fsize = fsize;
	seg->next_offset = -1;
	seg->next_len = -1;
	seg->next_fid = -1;
	seg->next_voffset = -1;
	memcpy(buf+r,seg,SEG_SIZE);
	free(seg);
	r += SEG_SIZE;
	printf("hehe, chunking done.\n");

	if(r > 0){
		cnt++;
		n = 0;
		while((w = write(connfd,buf+n,r-n))>0)
			n+=w;
		if ( w == -1) {
			perror("Write Request");
			exit(1);
		}

	}
	free(buf);
	fprintf(stderr,"%d chunks\n",cnt);
	return NULL;
}


/**
 * Receive deduplication and distribution results from metadata server
 * \param	ptr	instance of pArgs
 * \return 	NULL
 */
void* response(void* ptr)
{
	pArgs args = *(pArgs*)ptr;
	int connfd = *(int*)(args.fd);
	char *buf=(char*)malloc(sizeof(char)*BUF_SIZE);
	Segment* seg;
	uint64_t stat[ARRAY_SIZE];
	uint64_t byte_stat[ARRAY_SIZE];
	memset(byte_stat,0,sizeof(uint64_t)*ARRAY_SIZE);
	memset(stat,0,sizeof(uint64_t)*ARRAY_SIZE);


	//Receive Distribution Decision From Master
	int j,r,w,n = 0;
	int turn=-1,last_gid=-1;
	struct timeval a,b,c;
	gettimeofday(&a,NULL);
	while(( r = read(connfd,buf+n,BUF_SIZE-n)) > 0){
		n += r;
		if (n == BUF_SIZE){
			for(w=0;w<BUF_SIZE;w+=SEG_SIZE){
				seg = (Segment*) malloc(sizeof(Segment));
				memcpy(seg,buf+w,SEG_SIZE);
				if (seg->pdisk[0] >= ARRAY_SIZE)
					fprintf(stderr,"ERROR: to disk %d\n",seg->pdisk[0]);
				stat[seg->disk]++;
				byte_stat[seg->disk] += seg->len;
				if(seg->id > 0){
					if (last_gid != seg->stid){
						turn++;
						last_gid = seg->stid;
						for (j=0;j<CODE_SIZE;j++){
							stat[seg->pdisk[j]]++;
							byte_stat[seg->pdisk[j]]+= MAX_SEG_SIZE;
						}
					}
					//fprintf(stderr,"Chunk%ld in stripe %ld to disk %d\n",seg->id, seg->stid, seg->disk);
#ifndef SIMULATION
					Enqueue((Queue*)(args.oq[turn%ENCODER_NUM]),seg);
#endif
				}
			}
			n = 0;
		}
	}
	if(n > 0){
		for(w=0;w<n;w+=SEG_SIZE){
			seg = (Segment*) malloc(sizeof(Segment));
			memcpy(seg,buf+w,SEG_SIZE);
			stat[seg->disk]++;
			byte_stat[seg->disk] += seg->len;
			if(seg->id > 0){
 				if (last_gid != seg->stid){
					turn++;
					last_gid = seg->stid;
					for (j=0;j<CODE_SIZE;j++){
						stat[seg->pdisk[j]]++;
						byte_stat[seg->pdisk[j]] += MAX_SEG_SIZE;
					}
				}
				//fprintf(stderr,"Chunk%ld in stripe %ld to disk %d\n",seg->id, seg->stid,seg->disk);
#ifndef SIMULATION
				Enqueue((Queue*)(args.oq[turn%ENCODER_NUM]),seg);
#endif
			}
		}
	}
	for(j=0;j<ENCODER_NUM;j++)
		Enqueue(args.oq[j],NULL);
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	free(buf);
	fprintf(stderr,"%ld.%06ld response exit ...\n",c.tv_sec,c.tv_usec);
	for (j=0;j<ARRAY_SIZE;j++){
		printf("Node %d: %lu blocks of %lu bytes\n",j,stat[j],byte_stat[j]);
	}
	return NULL;
}

/**
 * Main function
 */
int main(int argc, char** argv)
{
	signal(SIGPIPE,SIG_IGN);
	struct timeval a,b,c;
	gettimeofday(&a,NULL);
	if(argc != 6 && argc != 5){
		printf("Usages: \n \
				Upload File: %s <host> <port> <metafile> <raw file> [D|F]\n \
				Restore File: %s <host> <directory> <instance> <version>\n \
                ", argv[0],argv[0]);
		exit(1);
	} else if (argc == 6){
	host = argv[1];
	int port = atoi(argv[2]);
	int connfd = 0;
	struct sockaddr_in serv_addr;
	memset(&serv_addr,'0',sizeof(struct sockaddr_in));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = inet_addr(host);
	serv_addr.sin_port = htons(port);



	uint64_t fsize;
	uint8_t** rdata = (uint8_t**)malloc(sizeof(uint8_t*)*40000);
	char** files = (char**)malloc(sizeof(char*)*40000);
	uint64_t* ss = (uint64_t*)malloc(sizeof(uint64_t)*40000);
	int map_cnt=0;
	int i;//,window=100;
	char prefix[256];
	int printed = 0;
	strcpy(prefix,argv[4]);
	strcat(prefix,"/");

	if(strcmp(argv[5],"F")==0){
		files[0] = (char*)malloc(sizeof(char)*256);
		memcpy(files[0],argv[4],strlen(argv[4]));
		map_cnt=1;
	} else {
		recur_file_list(argv[4],files,&map_cnt,prefix);
		fprintf(stderr,"hehe %d\n",map_cnt);
	}


	chdir(WORK_DIR);

	for(i=0;i<map_cnt;i+=WINDOW_SIZE){
		printf ("segment size = %ld\n", sizeof(Segment));
		pthread_t _request, _response;
		//pthread_t _encoder[4];

		connfd = socket(AF_INET,SOCK_STREAM,0);

		if(connect(connfd,(struct sockaddr *)&serv_addr, sizeof(struct sockaddr)) < 0){
			printf("\nError : Connect Failed \n");
			exit(1);
		}
		fsize = 0;

		int j,k;
		int end = MIN(map_cnt,i+WINDOW_SIZE);
		for(j=i;j<end;j++){
			int rfd = open(files[j],O_RDONLY);
			uint64_t sz = lseek(rfd,0,SEEK_END);
			fsize += sz;
			if (fsize > 9999999999 && printed == 0){
				fprintf(stderr,"File: %s of size %lu\n",files[j],sz);
				printed = 1;
			}
			rdata[j-i] = (uint8_t*)mmap(0,sz,PROT_READ,MAP_SHARED,rfd,0);
			ss[j-i] = sz;
			close(rfd);
		}


		Queue* cq = NewQueue();
		matrix  = reed_sol_vandermonde_coding_matrix(STRIPE_SIZE-CODE_SIZE,CODE_SIZE,8);
		pthread_t wtid[DISK_NUM];
		pArgs wargs[DISK_NUM];
		memset(wtid,0,sizeof(pthread_t)*DISK_NUM);
		memset(wargs,0,sizeof(pArgs)*DISK_NUM);
		for(j=0;j<DISK_NUM;j++){
			wargs[j].id = j;
			wargs[j].q = NewQueue();
			pthread_create(&wtid[j],NULL,writer,&(wargs[j]));
		}

		pthread_t ctid[ENCODER_NUM];
		pArgs cargs[ENCODER_NUM];
		memset(ctid,0,sizeof(pthread_t)*ENCODER_NUM);
		memset(cargs,0,sizeof(pArgs)*ENCODER_NUM);
		for(j=0;j<ENCODER_NUM;j++){
			cargs[j].id = fsize;
			cargs[j].oq = (Queue**)malloc(sizeof(Queue*)*DISK_NUM);
			for (k=0;k<DISK_NUM;k++) cargs[j].oq[k] = wargs[k].q;
			cargs[j].q = NewQueue();
			cargs[j].data = (void**)rdata;
			cargs[j].dcnt = &ss[0];
			pthread_create(&ctid[j],NULL,encoder,&(cargs[j]));
		}
		Chunker* chk = GetChunker();
#ifdef WITH_REAL_DATA
		chk->start(files,i,end,cq);
#else
		fprintf(stderr,"chunk %s\n",argv[3]);
		chk->start(&argv[3],0,1,cq);
#endif


		pArgs req, res;
		memset(&req,0,sizeof(pArgs));
		memset(&res,0,sizeof(pArgs));
		req.id = fsize;
		req.q = cq;
		req.fd = &connfd;
		res.fd = &connfd;
		res.oq = (Queue**)malloc(sizeof(Queue*)*ENCODER_NUM);
		for(j=0;j<ENCODER_NUM;j++){
			res.oq[j] = cargs[j].q;
		}

	pthread_create(&_request,NULL,request,&req);
	pthread_create(&_response,NULL,response,&res);

	chk->stop();
	printf("Chunking to segments done.\n");

	pthread_join(_request,NULL);
	pthread_join(_response,NULL);

	for(j=0;j<ENCODER_NUM;j++)
		pthread_join(ctid[j],NULL);

	for(j=0;j<DISK_NUM;j++){
		Enqueue(wargs[j].q,NULL);
		pthread_join(wtid[j],NULL);
	}

	free(res.data);
	free(matrix);
	for(j=i;j<end;j++){
		munmap(rdata[j-i],ss[j-i]);
	}
	free(cq);
	}

	for(i=0;i<map_cnt;i++){
		free(files[i]);
	}
	free(rdata);
	free(files);
	free(ss);

	} else if (argc == 5){
		list* result;
		path* pth;
		image* img;
		int i,j;
		Size* fsize;
		uint64_t segs;


		reader_synced = 0;
		//Initiate Failure status
		memset(failed,0,sizeof(int)*ARRAY_SIZE);

		memset(decoding,0,sizeof(int)*DECODER_NUM);

		//Bitmap to filter out duplicate chunk
		uint8_t* bitmap = (uint8_t*)malloc(sizeof(uint8_t)*2*134217728);
		memset(bitmap,0,sizeof(uint8_t)*2*134217728);


		char buf[64];
		sprintf(buf,"/run/shm/%d-%u",atoi(argv[3]),atoi(argv[4]));
		int fd = creat(buf,0644);
		if(fd == -1){
			printf("Unable to create file.\n");
			return 1;
		}
		sprintf(buf,"%u/%d-%u",atoi(argv[2]),atoi(argv[3]),atoi(argv[4]));

		matrix  = reed_sol_vandermonde_coding_matrix(STRIPE_SIZE-CODE_SIZE,CODE_SIZE,8);
		//Start Decoders
		pthread_t dtid[DECODER_NUM];
		pArgs dargs[DECODER_NUM];
		memset(dtid,0,sizeof(pthread_t)*DECODER_NUM);
		memset(dargs,0,sizeof(pArgs)*DECODER_NUM);
		for(j=0;j<DECODER_NUM;j++){
			dargs[j].id = j;
			dargs[j].q = NewQueue();
			dargs[j].fd = &fd;
			pthread_create(&dtid[j],NULL,decoder,&(dargs[j]));
		}

		//Start Readers
		//Bind output queues of each reader to all the decoders' input queue
		pthread_t tid[DISK_NUM];
		pArgs args[DISK_NUM];
		for(i=0;i<DISK_NUM;i++){
			args[i].id = i;
			args[i].q = NewQueue();
			args[i].oq = (Queue**)malloc(sizeof(Queue*)*DECODER_NUM);
			for (j=0;j<DECODER_NUM;j++) args[i].oq[j] = dargs[j].q;
			args[i].fd = &fd;
			pthread_create(&tid[i],NULL,reader,&(args[i]));
		}

		CLIENT *cl = clnt_create(argv[1],FETCHER, FETCHER_V1, "tcp");
		if(cl == NULL){
			printf("error: could not connect to server.\n");
			return 1;
		}

		//Query file size from master
		//Truncate the file to queried file size
		img = (image*)malloc(sizeof(img));
		img->ins = atoi(argv[3]);
		img->vers = atoi(argv[4]);
		fsize = fetch_size_1(img,cl);
		segs = fsize->chks;
		printf("%ld segments to download\n",segs);
		assert(!ftruncate(fd,fsize->bytes));
#ifdef CHECK_READ
		int orig_fd = open("test/test",O_RDONLY);
		assert(orig_fd != -1);
		orig_file = MMAP_FD_RO(orig_fd,fsize->bytes);
		close(orig_fd);
#endif

		//Fetch segment list from master
		//Download segments concurrently from all slaves
		pth = (path*)malloc(sizeof(path));
		pth->name = buf;
		int turn = 0;
#ifndef SIMULATION
		//Wait for all readers to update status
		pthread_mutex_lock(&reader_sync_mutex);
		while ( reader_synced != ARRAY_SIZE )
			pthread_cond_wait(&reader_sync_cond,&reader_sync_mutex);
		//fprintf(stderr,"Reader Status:\n");
		//for (i=0;i<ARRAY_SIZE;i++)
		//	fprintf(stderr,"%d,",failed[i]);
		//fprintf(stderr,"\n");
		pthread_mutex_unlock(&reader_sync_mutex);
#endif
		for (i=0;i<segs;i+=32768){
			//cnt = 0;
			pth->start = i;
			pth->end = (segs-i > 32768)?i+32768:segs;

			result = fetch_list_1(pth,cl);
			if (result == NULL) {
	    		printf("error: RPC failed!\n");
	    		return 1;
			}
			list* ptr;
			ptr = result;
			while(ptr != NULL){

				if(ptr->disk > ARRAY_SIZE-1)
					printf("Disk%d\n",ptr->disk);
				assert(ptr->disk < ARRAY_SIZE);
				int st_cnt;

				if (failed[ptr->disk] == 1){
					//Disk where the needed chunk resides fails
					//1. Enqueue download requests for other chunks in the
					//   stripe
					//2. Reconstruct required chunks using other chunks of the
					//	 same stripe, and write re-constructed chunk to file
					st_cnt = 0;
					fprintf(stderr,"Chunk %ld on disk %d,%d[",ptr->id,ptr->disk,ptr->stlen);
					uint8_t tindex,stlen=0;
					for (j=0;j<(STRIPE_SIZE-CODE_SIZE);j++){
						if (ptr->st_chk[j].id == ptr->id)
							tindex = j;
						if (failed[ptr->st_chk[j].disk] == 0 && ptr->st_chk[j].id != 0)
							stlen++;
					}

					for (j=0;j<STRIPE_SIZE;j++){
						if ( failed[ptr->st_chk[j].disk] == 0 && ptr->st_chk[j].id != 0){
							QEntry *qtmp = (QEntry*)malloc(sizeof(QEntry));
							qtmp->id = ptr->st_chk[j].id;
							qtmp->offset = ptr->offset;
							qtmp->disk = ptr->st_chk[j].disk;
							if (j < STRIPE_SIZE-CODE_SIZE)
								qtmp->len = ptr->st_chk[j].len;
							else
								qtmp->len = MAX_SEG_SIZE;
							qtmp->dlen = ptr->len;
							qtmp->degraded = 1;
							qtmp->target = tindex;
							qtmp->index = j;
							qtmp->stlen = ptr->stlen;
							fprintf(stderr,"%d,",qtmp->index);
							qtmp->dec_id = turn%DECODER_NUM;
							pthread_mutex_lock(&(decode_lock[qtmp->dec_id]));
							decoding[turn%DECODER_NUM]++;
							pthread_mutex_unlock(&(decode_lock[qtmp->dec_id]));
						if (((bitmap[qtmp->id/8] & ( 1 << qtmp->id%8 )) >> qtmp->id%8) == 0 ) {
							pthread_mutex_unlock(&(decode_lock[qtmp->dec_id]));
						if (((bitmap[qtmp->id/8] & ( 1 << qtmp->id%8 )) >> qtmp->id%8) == 0 ) {
							bitmap[qtmp->id/8] += 1<<qtmp->id%8;
							//Required chunk has not been encountered before
							qtmp->unique = 1;
						} else {
						}
							qtmp->unique = 0;
						}
							Enqueue(args[qtmp->disk].q,qtmp);
							if ( ++st_cnt == MIN(STRIPE_SIZE-CODE_SIZE,ptr->stlen)) break;
						}
					}
					fprintf(stderr,"], TURN: %d \n",turn%DECODER_NUM);
					turn++;

					if (turn%DECODER_NUM == 0) {
					//Wait until last batch of decoding is done
					for (j=0;j<DECODER_NUM;j++){
						while (decoding[j] > 0)
						fprintf(stderr,"Decoding[%d]: %d\n",j,decoding[j]);
						while (decoding[j] > 0)
							pthread_cond_wait(&(decode_cond[j]),&(decode_lock[j]));
						pthread_mutex_unlock(&(decode_lock[j]));
					}
					}
			} else {
					//Disk where the needed chunk resides is alive
					QEntry* qtmp = (QEntry*)malloc(sizeof(QEntry));
					qtmp->id = ptr->id;
					qtmp->offset = ptr->offset;
					qtmp->disk = ptr->disk;

					qtmp->degraded = 0;

					if (((bitmap[ptr->id/8] & ( 1 << ptr->id%8 )) >> ptr->id%8) == 0 ) {
						bitmap[ptr->id/8] += 1<<ptr->id%8;
						//Required chunk has not been encountered before
						qtmp->unique = 1;
						Enqueue(args[ptr->disk].q,qtmp);
					} else {
						//Required chunks has been downloaded before
						qtmp->unique = 0;
						Enqueue(args[ptr->disk].q,qtmp);
					}
				}
				if (ptr->disk >= ARRAY_SIZE) fprintf(stderr,"ERROR: disk %d\n", ptr->disk);
				ptr = ptr->next;
			}
			//fprintf(stderr,"%d chunks from disk 0\n",cnt);
		}

			Enqueue(args[i].q,NULL);

		for(i=0;i<DISK_NUM;i++)
			pthread_join(tid[i],NULL);

		for (i=0;i<DECODER_NUM;i++){
			Enqueue(dargs[i].q,NULL);
			pthread_join(dtid[i],NULL);
		}

#ifdef CHECK_READ
		munmap(orig_file,fsize);
#endif
		free(pth);
		free(bitmap);
		free(img);
		close(fd);
		free(matrix);
	}
		gettimeofday(&b,NULL);
		timersub(&b,&a,&c);
		printf("Total Time: %ld.%06ld\n",c.tv_sec,c.tv_usec);

	return 0;
}
