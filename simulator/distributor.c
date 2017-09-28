/**
 * @file 	distributor.c
 * @brief	Distribution Service Implementation
 * @author	Xu Min
 */
#include "distributor.h"

//This line is just to exercise my local sync and remote sync commands and write it in the Wiz

static Distributor dist;

typedef unsigned long ULONG;


static inline ULONG b(int n, int k){
	    if(n < k) return 0;
	    ULONG r = 1, d = n - k;

	    /* choose the smaller of k and n - k */
	    if (d > k) { k = d; d = n - k; }

	    while (n > k) {
	        if (r >= UINT_MAX / n) return 0; /* overflown */
	            r *= n--;

	            /* divide (n - k)! as soon as we can
				 * to delay overflows */
		        while (d > 1 && !(r % d)) r /= d--;
	    }
	    return r;
}
/*
static FList* NewLEntry(uint64_t fid){
	FList* flen = (FList*)malloc(sizeof(FList));
	assert ( flen != NULL);
	flen->next = NULL;
	flen->en.id = fid;
	memset(&flen->en.stat,0,sizeof(uint64_t)*ARRAY_SIZE);
	flen->segs = 0;
	if(dist._head == NULL){
		dist._head = flen;
		dist._tail = flen;
	} else {
		dist._tail->next = flen;
		dist._tail = flen;
	}

	return flen;
}
*/
/*
static void Baseline(Segment** sbuf,int num){
	int i;
	for(i=0;i<num;i++){
		int disk = dist._sb->GetDisk(i);
		dist._sb->Insert(disk);
		sbuf[i]->disk = disk;

		kcdbadd(dist._map,(const char*)&(sbuf[i]->id),sizeof(uint64_t),(const char*)&(sbuf[i]->disk),sizeof(uint8_t));
		Enqueue(dist._oq, sbuf[i]);
	}
}
*/
static void Basline(Segment** sbuf, int num,int did){
	struct PlacementInfor* placement;//(struct PlacementInfor*)malloc(sizeof(struct PlacementInfor)*num);
	int i,j;
	int lgid=-1,st_cnt=0;
	//char *err = NULL;
	//fprintf(stderr,"Num of chunks in segment %d, num of files %d, size of R %d\n",num,dist._fSize[did],dist._sizeofR[did]);	

	placement = BASLINE(b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE*STRIPE_SIZE/ARRAY_SIZE,ARRAY_SIZE,dist._f[did],dist._fSize[did],dist._DArray[did],dist._RArray[did], dist._sizeofR[did],STRIPE_SIZE-CODE_SIZE,CODE_SIZE);
	for (i=0;i<dist._sizeofR[did];i++)
		dist._CDArray[did][dist._RArray[did][i].ab_file_id*ARRAY_SIZE+placement[dist._RArray[did][i].block_id].node_id]++;

    for (i=0;i<num;i++){
		if(sbuf[i]->unique == 1){
			sbuf[i]->disk =placement[i].node_id; 
			sbuf[i]->stid = placement[i].gid;

			//Update Stripe Metadata
			if (lgid != sbuf[i]->stid){
				dist._stlog->id++;
				for (j=0;j<CODE_SIZE;j++){
					pthread_mutex_lock(&dist._lock);
					dist._sten[dist._stlog->id].ddisk[j+STRIPE_SIZE-CODE_SIZE] = placement[i].parities[j];
					dist._sten[dist._stlog->id].size[j+STRIPE_SIZE-CODE_SIZE] = MAX_SEG_SIZE;//sbuf[i]->len;
					dist._sten[dist._stlog->id].sid[STRIPE_SIZE-CODE_SIZE+j] = --dist._slog->segID;
					pthread_mutex_unlock(&dist._lock);
					pthread_mutex_lock(dist._csmap_lock);
					kcdbset(dist._csmap,(const char*)&dist._slog->segID,sizeof(uint64_t),(const char*)&dist._stlog->id,sizeof(uint64_t));
					pthread_mutex_unlock(dist._csmap_lock);
				}
				lgid = sbuf[i]->stid;
				//fprintf(stderr,"\n");
				st_cnt=0;
			}
			st_cnt++;
			pthread_mutex_lock(&dist._lock);
			dist._sten[dist._stlog->id].id = dist._stlog->id;
			dist._sten[dist._stlog->id].len = st_cnt;
			dist._sten[dist._stlog->id].sid[st_cnt-1] = sbuf[i]->id;
			dist._sten[dist._stlog->id].size[st_cnt-1] = sbuf[i]->len;
			dist._sten[dist._stlog->id].ddisk[st_cnt-1] = sbuf[i]->disk;
			//fprintf(stderr,"D%d:",dist._sten[dist._stlog->id].ddisk[st_cnt-1]);
			sbuf[i]->stid = dist._stlog->id;
			pthread_mutex_unlock(&dist._lock);
			pthread_mutex_lock(dist._csmap_lock);
			kcdbset(dist._csmap,(const char*)&sbuf[i]->id,sizeof(uint64_t),(const char*)&sbuf[i]->stid,sizeof(uint64_t));
			pthread_mutex_unlock(dist._csmap_lock);
			for (j=0;j<CODE_SIZE;j++){
				sbuf[i]->pdisk[j] = placement[i].parities[j];
				if (placement[i].parities[j] > ARRAY_SIZE)
					fprintf(stderr,"Chunk %d parity %d to disk %d\n",i,j,placement[i].parities[j]);
				pthread_mutex_lock(&dist._lock);
				sbuf[i]->pid[j] = dist._sten[dist._stlog->id].sid[STRIPE_SIZE-CODE_SIZE+j];
				pthread_mutex_unlock(&dist._lock);
				//fprintf(stderr,"P%d,",placement[i].parities[j]);
			}

			//fprintf(stderr,"%d(%d,%d,%d,%d),",sbuf[i]->disk,placement[i].offset,placement[i].gid,placement[i].parities[0],placement[i].parities[1]);
			//if ((i+1)%(STRIPE_SIZE-CODE_SIZE) == 0) fprintf(stderr,"\n");
			//sbuf[i]->disk = 0;
			//fprintf(stderr,"seg %ld to disk %d\n",sbuf[i]->id, sbuf[i]->disk);
			pthread_mutex_lock(dist._map_lock);
			kcdbset(dist._map,(char*)&sbuf[i]->id,sizeof(uint64_t),(char*)&sbuf[i]->disk,sizeof(uint8_t));
			pthread_mutex_unlock(dist._map_lock);
			//leveldb_put(dist._lmap,dist._woptions,(const char*)&sbuf[i]->id,sizeof(uint64_t),(const char*)&sbuf[i]->disk,sizeof(uint8_t),&err);
			dist._CDArray[did][sbuf[i]->fid*ARRAY_SIZE+sbuf[i]->disk]++;
			//dist._CPDArray[did][sbuf[i]->fid*ARRAY_SIZE*ARRAY_SIZE+sbuf[i]->disk*ARRAY_SIZE+sbuf[i]->pdisk[0]]++;
		}
		Enqueue(dist._oq[did], sbuf[i]);
	}
	
	free(placement);
/*
	pthread_mutex_lock(&map_lock);
	kcdbdumpsnap(dist._map, "meta/sdmap");
	kcdbclose(dist._map);
	kcdbdel(dist._map);

	dist._map = kcdbnew();
	kcdbopen(dist._map, "-", KCOWRITER | KCOCREATE);
	kcdbloadsnap(dist._map, "meta/sdmap");

	pthread_mutex_unlock(&map_lock);
	*/
}

static void RADBased(Segment** sbuf, int num,int did){
	struct PlacementInfor* placement;//(struct PlacementInfor*)malloc(sizeof(struct PlacementInfor)*num);
	int i,j;
	int lgid=-1,st_cnt=0;
	//char *err = NULL;
	//fprintf(stderr,"Num of chunks in segment %d, num of files %d, size of R %d\n",num,dist._fSize[did],dist._sizeofR[did]);	
	printf("now we are in RAD mode...\n");
	placement = BASLINE(b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE*STRIPE_SIZE/ARRAY_SIZE,ARRAY_SIZE,dist._f[did],dist._fSize[did],dist._DArray[did],dist._RArray[did], dist._sizeofR[did],STRIPE_SIZE-CODE_SIZE,CODE_SIZE);

	int p, q;
	Segment* t;
		for(p =0; p < (num-1); p++)
		{
			for(q = 0; q <= (num-2-p); q++)
				
				if(dist._sen[sbuf[q]->id].ref>dist._sen[sbuf[q+1]->id].ref)           
	            {  
	                t=sbuf[q];  
	                sbuf[q]=sbuf[q+1];  
	                sbuf[q+1]=t;  
	            }  
		}

	for (i=0;i<dist._sizeofR[did];i++)
		dist._CDArray[did][dist._RArray[did][i].ab_file_id*ARRAY_SIZE+placement[dist._RArray[did][i].block_id].node_id]++;

    for (i=0;i<num;i++){
		if(sbuf[i]->unique == 1){
			sbuf[i]->disk =placement[i].node_id; 
			sbuf[i]->stid = placement[i].gid;

			//Update Stripe Metadata
			if (lgid != sbuf[i]->stid){
				dist._stlog->id++;
				for (j=0;j<CODE_SIZE;j++){
					pthread_mutex_lock(&dist._lock);
					dist._sten[dist._stlog->id].ddisk[j+STRIPE_SIZE-CODE_SIZE] = placement[i].parities[j];
					dist._sten[dist._stlog->id].size[j+STRIPE_SIZE-CODE_SIZE] = MAX_SEG_SIZE;//sbuf[i]->len;
					dist._sten[dist._stlog->id].sid[STRIPE_SIZE-CODE_SIZE+j] = --dist._slog->segID;
					pthread_mutex_unlock(&dist._lock);
					pthread_mutex_lock(dist._csmap_lock);
					kcdbset(dist._csmap,(const char*)&dist._slog->segID,sizeof(uint64_t),(const char*)&dist._stlog->id,sizeof(uint64_t));
					pthread_mutex_unlock(dist._csmap_lock);
				}
				lgid = sbuf[i]->stid;
				//fprintf(stderr,"\n");
				st_cnt=0;
			}
			st_cnt++;
			pthread_mutex_lock(&dist._lock);
			dist._sten[dist._stlog->id].id = dist._stlog->id;
			dist._sten[dist._stlog->id].len = st_cnt;
			dist._sten[dist._stlog->id].sid[st_cnt-1] = sbuf[i]->id;
			dist._sten[dist._stlog->id].size[st_cnt-1] = sbuf[i]->len;
			dist._sten[dist._stlog->id].ddisk[st_cnt-1] = sbuf[i]->disk;
			//fprintf(stderr,"D%d:",dist._sten[dist._stlog->id].ddisk[st_cnt-1]);
			sbuf[i]->stid = dist._stlog->id;
			pthread_mutex_unlock(&dist._lock);
			pthread_mutex_lock(dist._csmap_lock);
			kcdbset(dist._csmap,(const char*)&sbuf[i]->id,sizeof(uint64_t),(const char*)&sbuf[i]->stid,sizeof(uint64_t));
			pthread_mutex_unlock(dist._csmap_lock);
			for (j=0;j<CODE_SIZE;j++){
				sbuf[i]->pdisk[j] = placement[i].parities[j];
				if (placement[i].parities[j] > ARRAY_SIZE)
					fprintf(stderr,"Chunk %d parity %d to disk %d\n",i,j,placement[i].parities[j]);
				pthread_mutex_lock(&dist._lock);
				sbuf[i]->pid[j] = dist._sten[dist._stlog->id].sid[STRIPE_SIZE-CODE_SIZE+j];
				pthread_mutex_unlock(&dist._lock);
				//fprintf(stderr,"P%d,",placement[i].parities[j]);
			}

			//fprintf(stderr,"%d(%d,%d,%d,%d),",sbuf[i]->disk,placement[i].offset,placement[i].gid,placement[i].parities[0],placement[i].parities[1]);
			//if ((i+1)%(STRIPE_SIZE-CODE_SIZE) == 0) fprintf(stderr,"\n");
			//sbuf[i]->disk = 0;
			//fprintf(stderr,"seg %ld to disk %d\n",sbuf[i]->id, sbuf[i]->disk);
			pthread_mutex_lock(dist._map_lock);
			kcdbset(dist._map,(char*)&sbuf[i]->id,sizeof(uint64_t),(char*)&sbuf[i]->disk,sizeof(uint8_t));
			pthread_mutex_unlock(dist._map_lock);
			//leveldb_put(dist._lmap,dist._woptions,(const char*)&sbuf[i]->id,sizeof(uint64_t),(const char*)&sbuf[i]->disk,sizeof(uint8_t),&err);
			dist._CDArray[did][sbuf[i]->fid*ARRAY_SIZE+sbuf[i]->disk]++;
			//dist._CPDArray[did][sbuf[i]->fid*ARRAY_SIZE*ARRAY_SIZE+sbuf[i]->disk*ARRAY_SIZE+sbuf[i]->pdisk[0]]++;
		}
		Enqueue(dist._oq[did], sbuf[i]);
	}
	
	free(placement);
/*
	pthread_mutex_lock(&map_lock);
	kcdbdumpsnap(dist._map, "meta/sdmap");
	kcdbclose(dist._map);
	kcdbdel(dist._map);

	dist._map = kcdbnew();
	kcdbopen(dist._map, "-", KCOWRITER | KCOCREATE);
	kcdbloadsnap(dist._map, "meta/sdmap");

	pthread_mutex_unlock(&map_lock);
	*/
}
/*
static void GroupGreedy(Segment** sbuf, int num){
	int i,j;
	for(i=0;i<num;i++){
		if(sbuf[i]->unique == 1){
			int disk,min=99999;
			for(j=0;j<ARRAY_SIZE;j++)
				if(dist.stat[j] < min){
					min = dist.stat[j];
					disk=j;
				}
			sbuf[i]->disk = disk;
			kcdbadd(dist._map,(char*)&sbuf[i]->id,sizeof(uint64_t),(char*)&sbuf[i]->disk,sizeof(uint8_t));
			dist.stat[disk]++;
		}
		Enqueue(dist._oq,sbuf[i]);
	}
}


static void StripeBased(Segment** sbuf, int num){
	int i,j;
	int ldisk=-1;
	uint64_t size;
	for(i=0;i<num;i++){
		if(sbuf[i]->unique == 1){
			int disk,min=99999;
			for(j=0;j<ARRAY_SIZE;j++)
				if(dist.stat[j] < min && j != ldisk){
					min = dist.stat[j];
					disk=j;
				}
			sbuf[i]->disk = disk;
			kcdbadd(dist._map,(char*)&sbuf[i]->id,sizeof(uint64_t),(char*)&sbuf[i]->disk,sizeof(uint8_t));
			dist.stat[disk]++;
		}
		ldisk = *(uint8_t*)kcdbget(dist._map,(char*)&sbuf[i]->id,sizeof(uint64_t),&size);
		Enqueue(dist._oq,sbuf[i]);
	}
}


static void SeekBased(Segment** sbuf, int num){
	int i,j;
	for(i=0;i<num;i++){
		if(sbuf[i]->unique == 1){
			int disk,min=99999;
			for(j=0;j<ARRAY_SIZE;j++)
				if(dist.stat[j] < min){
					min = dist.stat[j];
					disk=j;
				}
			sbuf[i]->disk = disk;
			kcdbadd(dist._map,(char*)&sbuf[i]->id,sizeof(uint64_t),(char*)&sbuf[i]->disk,sizeof(uint8_t));
			dist.stat[disk]++;
		}
		Enqueue(dist._oq,sbuf[i]);
	}
}
*/
static void EDPBased(Segment** sbuf, int num,int did){
	struct PlacementInfor* placement;//(struct PlacementInfor*)malloc(sizeof(struct PlacementInfor)*num);
	int i,j;
	int lgid=-1,st_cnt=0;
	//fprintf(stderr,"Num of chunks in segment %d\n",num);	
	
	placement = EDP(b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE*STRIPE_SIZE/ARRAY_SIZE,ARRAY_SIZE,dist._f[did],dist._fSize[did],dist._DArray[did],dist._PDArray[did],dist._RArray[did], dist._sizeofR[did],STRIPE_SIZE-CODE_SIZE,CODE_SIZE);

	Segment** tmp = (Segment**)malloc(sizeof(Segment*)*SEG_BUF_SIZE);
	memset(tmp,0,sizeof(Segment*)*SEG_BUF_SIZE);
	uint8_t* stat = (uint8_t*)malloc(sizeof(uint8_t)*b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE);
	memset(stat,0,sizeof(uint8_t)*b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE);
	assert(tmp != NULL);
	struct timeval a,d,c;
	gettimeofday(&a,NULL);
    for (i=0;i<num;i++){
		//if(sbuf[i]->unique == 1){
		sbuf[i]->disk = placement[i].node_id;
		sbuf[i]->stid = placement[i].gid;
		for (j=0;j<CODE_SIZE;j++) {
			sbuf[i]->pdisk[j] = placement[i].parities[j];
			if (sbuf[i]->pdisk[j] >= ARRAY_SIZE)
				fprintf(stderr,"[DIST]block %d: parity %d to disk %d\n",i,j,placement[i].parities[j]);
		}
		tmp[placement[i].gid*(STRIPE_SIZE-CODE_SIZE)+stat[placement[i].gid]] = sbuf[i];
		stat[placement[i].gid] += 1;
		//fprintf(stderr,"File%ld: %d\n",sbuf[i]->fid,sbuf[i]->disk);
		pthread_mutex_lock(dist._map_lock);
		kcdbadd(dist._map,(char*)&(sbuf[i]->id),sizeof(uint64_t),(char*)&(sbuf[i]->disk),sizeof(uint8_t));
		pthread_mutex_unlock(dist._map_lock);
		dist._CDArray[did][sbuf[i]->fid*ARRAY_SIZE+sbuf[i]->disk]++;
		//dist._CPDArray[did][sbuf[i]->fid*ARRAY_SIZE*ARRAY_SIZE+sbuf[i]->disk*ARRAY_SIZE+sbuf[i]->pdisk[0]]++;
		//}
	}
	//for (i=0;i<b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE;i++)
	//	fprintf(stderr,"%d strips in %d-th stripe\n",stat[i],i);
	
	int tmp_cnt=0;
	for (i=0;i<b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE*(STRIPE_SIZE-CODE_SIZE);i++)
		if(tmp[i]) {
			//Update Stripe Metadata
			if (lgid != tmp[i]->stid){
				dist._stlog->id++;
				for (j=0;j<CODE_SIZE;j++){
					pthread_mutex_lock(&dist._lock);
					dist._sten[dist._stlog->id].ddisk[j+STRIPE_SIZE-CODE_SIZE] = tmp[i]->pdisk[j];
					dist._sten[dist._stlog->id].size[j+STRIPE_SIZE-CODE_SIZE] = MAX_SEG_SIZE;//sbuf[i]->len;
					dist._sten[dist._stlog->id].sid[STRIPE_SIZE-CODE_SIZE+j] = --dist._slog->segID;
					pthread_mutex_unlock(&dist._lock);
					pthread_mutex_lock(dist._csmap_lock);
					kcdbset(dist._csmap,(const char*)&dist._slog->segID,sizeof(uint64_t),(const char*)&dist._stlog->id,sizeof(uint64_t));
					pthread_mutex_unlock(dist._csmap_lock);
				}
				lgid = tmp[i]->stid;
				//fprintf(stderr,"\n");
				st_cnt=0;
			}
			st_cnt++;
			pthread_mutex_lock(&dist._lock);
			dist._sten[dist._stlog->id].id = dist._stlog->id;
			dist._sten[dist._stlog->id].len = st_cnt;
			dist._sten[dist._stlog->id].sid[st_cnt-1] = tmp[i]->id;
			dist._sten[dist._stlog->id].size[st_cnt-1] = tmp[i]->len;
			dist._sten[dist._stlog->id].ddisk[st_cnt-1] = tmp[i]->disk;
			//fprintf(stderr,"D%d:",dist._sten[dist._stlog->id].ddisk[st_cnt-1]);
			tmp[i]->stid = dist._stlog->id;
			pthread_mutex_unlock(&dist._lock);
			pthread_mutex_lock(dist._csmap_lock);
			kcdbset(dist._csmap,(const char*)&tmp[i]->id,sizeof(uint64_t),(const char*)&tmp[i]->stid,sizeof(uint64_t));
			pthread_mutex_unlock(dist._csmap_lock);
			for (j=0;j<CODE_SIZE;j++){
				//if (tmp[i]->pdisk[j] >= ARRAY_SIZE)
				//	fprintf(stderr,"[DIST]block %d: parity %d to disk %d\n",i,j,tmp[i]->pdisk[j]);
				pthread_mutex_lock(&dist._lock);
				tmp[i]->pid[j] = dist._sten[dist._stlog->id].sid[STRIPE_SIZE-CODE_SIZE+j];
				pthread_mutex_unlock(&dist._lock);
				//fprintf(stderr,"P%d,",tmp[i]->pdisk[j]);
			}
			if (tmp[i]->fid == 7)
				tmp_cnt++;

			Enqueue(dist._oq[did], tmp[i]);
			//fprintf(stderr,"%d(%d,%d,%d),",tmp[i]->disk,tmp[i]->stid,tmp[i]->pdisk[0],tmp[i]->pdisk[1]);
        	//if ((i+1)%(STRIPE_SIZE-CODE_SIZE) == 0) fprintf(stderr,"\n");
		}
	gettimeofday(&d,NULL);
	timersub(&d,&a,&c);
	fprintf(stderr,"EDP Enqueue time: %ld.%06ld, File 7: %d\n",c.tv_sec,c.tv_usec,tmp_cnt);
	
	free(stat);
	free(tmp);
	free(placement);
}

static void CEDPBased(Segment** sbuf, int num,int did){
	struct PlacementInfor* placement;
	int i,j;
	int lgid=-1,st_cnt=0,cnt=0;
	//NOTE: cost array contains link bandwidth of each node
	placement = CEDP(b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE*STRIPE_SIZE/ARRAY_SIZE,ARRAY_SIZE,dist._f[did],dist._fSize[did],dist._DArray[did],dist._RArray[did], dist._cost, dist._sizeofR[did],STRIPE_SIZE-CODE_SIZE,CODE_SIZE);
	
	Segment** tmp = (Segment**)malloc(sizeof(Segment*)*SEG_BUF_SIZE);
	memset(tmp,0,sizeof(Segment*)*SEG_BUF_SIZE);
	uint8_t* stat = (uint8_t*)malloc(sizeof(uint8_t)*b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE);
	memset(stat,0,sizeof(uint8_t)*b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE);
	assert(tmp != NULL);

	for (i=0;i<num;i++){
		if(sbuf[i]->unique == 1){
		sbuf[i]->disk = placement[i].node_id;
		sbuf[i]->stid = placement[i].gid;
		for (j=0;j<CODE_SIZE;j++)
			sbuf[i]->pdisk[j] = placement[i].parities[j];
		tmp[placement[i].gid*(STRIPE_SIZE-CODE_SIZE)+stat[placement[i].gid]] = sbuf[i];
		stat[placement[i].gid] += 1;
		pthread_mutex_lock(dist._map_lock);
		kcdbadd(dist._map,(char*)&sbuf[i]->id,sizeof(uint64_t),(char*)&sbuf[i]->disk,sizeof(uint8_t));
		pthread_mutex_unlock(dist._map_lock);
		dist._CDArray[did][sbuf[i]->fid*ARRAY_SIZE+sbuf[i]->disk]++;
		//dist._CPDArray[did][sbuf[i]->fid*ARRAY_SIZE*ARRAY_SIZE+sbuf[i]->disk*ARRAY_SIZE+sbuf[i]->pdisk[0]]++;
		}
	}

	for (i=0;i<b(ARRAY_SIZE,STRIPE_SIZE)*STRIPE_SIZE*(STRIPE_SIZE-CODE_SIZE);i++)
		if(tmp[i] && tmp[i]->unique == 1) {
			//Update Stripe Metadata
			if (lgid != tmp[i]->stid){
				dist._stlog->id++;
				for (j=0;j<CODE_SIZE;j++){
					pthread_mutex_lock(&dist._lock);
					dist._sten[dist._stlog->id].ddisk[j+STRIPE_SIZE-CODE_SIZE] = tmp[i]->pdisk[j];
					dist._sten[dist._stlog->id].size[j+STRIPE_SIZE-CODE_SIZE] = MAX_SEG_SIZE;//sbuf[i]->len;
					dist._sten[dist._stlog->id].sid[STRIPE_SIZE-CODE_SIZE+j] = --dist._slog->segID;
					pthread_mutex_unlock(&dist._lock);
					pthread_mutex_lock(dist._csmap_lock);
					kcdbset(dist._csmap,(const char*)&dist._slog->segID,sizeof(uint64_t),(const char*)&dist._stlog->id,sizeof(uint64_t));
					pthread_mutex_unlock(dist._csmap_lock);
				}
				lgid = tmp[i]->stid;
				st_cnt=0;
			}
			st_cnt++;
			pthread_mutex_lock(&dist._lock);
			dist._sten[dist._stlog->id].id = dist._stlog->id;
			dist._sten[dist._stlog->id].len = st_cnt;
			dist._sten[dist._stlog->id].sid[st_cnt-1] = tmp[i]->id;
			dist._sten[dist._stlog->id].size[st_cnt-1] = tmp[i]->len;
			dist._sten[dist._stlog->id].ddisk[st_cnt-1] = tmp[i]->disk;
			tmp[i]->stid = dist._stlog->id;
			pthread_mutex_unlock(&dist._lock);
			pthread_mutex_lock(dist._csmap_lock);
			kcdbset(dist._csmap,(const char*)&tmp[i]->id,sizeof(uint64_t),(const char*)&tmp[i]->stid,sizeof(uint64_t));
			pthread_mutex_unlock(dist._csmap_lock);
			for (j=0;j<CODE_SIZE;j++){
				pthread_mutex_lock(&dist._lock);
				tmp[i]->pid[j] = dist._sten[dist._stlog->id].sid[STRIPE_SIZE-CODE_SIZE+j];
				pthread_mutex_unlock(&dist._lock);
			}

			Enqueue(dist._oq[did], tmp[i]);
		}

	/*
	for (i=0;i<dist._sizeofR[did];i++)
		dist._CDArray[did][dist._RArray[did][i].ab_file_id*ARRAY_SIZE+placement[dist._RArray[did][i].block_id].node_id]++;
	for(i=0;i<num;i++){
		if(sbuf[i]->unique == 1){
		sbuf[i]->disk = placement[i].node_id;
		pthread_mutex_lock(dist._map_lock);
		kcdbadd(dist._map,(char*)&sbuf[i]->id,sizeof(uint64_t),(char*)&sbuf[i]->disk,sizeof(uint8_t));
		pthread_mutex_unlock(dist._map_lock);
		dist._CDArray[did][sbuf[i]->fid*ARRAY_SIZE+sbuf[i]->disk]++;
		}
		Enqueue(dist._oq[did], sbuf[i]);
	}
	*/
	fprintf(stderr,"Enqueue %d chunks\n",cnt);
	
	free(stat);
	free(tmp);
	free(placement);
}

static void fEDPBased(Segment** sbuf, int num,int did){
	struct PlacementInfor* placement;
	int duplicate[ARRAY_SIZE];
	int storage_management[ARRAY_SIZE];
	memset(&storage_management[0],0,sizeof(int)*ARRAY_SIZE);
	int unique_num = 0;
	int i,j;
	uint64_t lfid=-1;
	int fcnt=0;
	int ccnt=0;
	fprintf(stderr,"enter fEDP, number %d\n",num);
	for (i=0;i<num;i++) {
		if (sbuf[i]->fid != lfid){
			if (lfid != -1) {
			placement = fEDP(ARRAY_SIZE,STRIPE_SIZE-CODE_SIZE,CODE_SIZE,sbuf[i-1]->fid,unique_num,duplicate,storage_management);
			for (j=i-unique_num;j<i;j++) {
				sbuf[j]->disk = placement[j-i+unique_num].node_id;
				pthread_mutex_lock(dist._map_lock);
				kcdbadd(dist._map,(char*)&sbuf[j]->id,sizeof(uint64_t),(char*)&sbuf[j]->disk,sizeof(uint8_t));
				pthread_mutex_unlock(dist._map_lock);
				dist._CDArray[did][sbuf[j]->fid*ARRAY_SIZE+sbuf[j]->disk]++;

				Enqueue(dist._oq[did],sbuf[j]);
			}
			ccnt += unique_num;
			free(placement);
			placement = NULL;
			}

			fcnt++;
			for (j=0;j<ARRAY_SIZE;j++) {
				duplicate[j] = dist._DArray[did][(fcnt-1)*ARRAY_SIZE+j];
			}
			unique_num = 1;
			lfid = sbuf[i]->fid;
		} else {
			unique_num++;
		}
	}
	if (ccnt < num) {
			placement = fEDP(ARRAY_SIZE,STRIPE_SIZE-CODE_SIZE,CODE_SIZE,sbuf[ccnt]->fid,unique_num,duplicate,storage_management);
			for (j=ccnt;j<num;j++) {
				sbuf[j]->disk = placement[j-ccnt].node_id;
				pthread_mutex_lock(dist._map_lock);
				kcdbadd(dist._map,(char*)&sbuf[j]->id,sizeof(uint64_t),(char*)&sbuf[j]->disk,sizeof(uint8_t));
				pthread_mutex_unlock(dist._map_lock);
				dist._CDArray[did][sbuf[j]->fid*ARRAY_SIZE+sbuf[j]->disk]++;
				Enqueue(dist._oq[did],sbuf[j]);
			}
			ccnt += unique_num;
			free(placement);
			placement = NULL;
	}

}


static void* process(void* ptr){
	int did = *(int*)ptr;
	free(ptr);
	Segment* seg = NULL;
	uint64_t size;
	uint64_t lids[ARRAY_SIZE];
	memset(lids,0,sizeof(uint64_t)*ARRAY_SIZE);
	Segment** sbuf = (Segment**)malloc(sizeof(Segment*)*SEG_BUF_SIZE);
	assert(sbuf != NULL);
	int cnt=0;
//	printf("the number of cnt is %d\n", cnt); 

	//FList* tmp = NULL;
	//int lfid = -1;
	int cfid = -1;
	int i,j,k;
	dist._fSize[did] = 0 ;
	struct timeval a,b,c;
	int tmp_cnt=0;
//	int while_loop_count=0;
	//Assume that files are processed in order and separately
	while((seg=(Segment*) Dequeue(dist._iq[did])) != NULL) {
		//printf("Distributor: chunk %ld of file %d, unique?%d\n",seg->id,seg->fid,seg->unique);
	//	printf("the reference counter of the chunk %ld is %d\n", seg->id, dist._sen[seg->id].ref);
		//fprintf(stderr,"Distributor: chunk %ld of file %d, unique?%d",seg->id,seg->fid,seg->unique);
		//Update fSize if segment with different fid is encountered
	//	while_loop_count++;
	//	printf("value of while loop count is %d\n", while_loop_count);
		if (seg->fid == 7) tmp_cnt++;
		if (cfid != seg->fid){
			
			//lfid = cfid;
			cfid = seg->fid;
			dist._fSize[did]++;
			int* tmpf = (int*)malloc(sizeof(int)*dist._fSize[did]);
			int* tmpd = (int*)malloc(sizeof(int)*dist._fSize[did]*ARRAY_SIZE);
			//int* tmppd = (int*)malloc(sizeof(int)*dist._fSize[did]*ARRAY_SIZE*ARRAY_SIZE);
			if (dist._fSize[did] > 1) {
				memcpy(tmpf,dist._f[did],sizeof(int)*(dist._fSize[did]-1));
				memcpy(tmpd,dist._DArray[did],sizeof(int)*(dist._fSize[did]-1)*ARRAY_SIZE);
				//memcpy(tmppd,dist._PDArray[did],sizeof(int)*(dist._fSize[did]-1)*ARRAY_SIZE*ARRAY_SIZE);
				free(dist._f[did]);
				free(dist._DArray[did]);
				//free(dist._PDArray[did]);
				dist._f[did] = NULL;
				dist._DArray[did] = NULL;
				//dist._PDArray[did] = NULL;
			}
			dist._f[did] = tmpf;
			dist._DArray[did] = tmpd;
			//dist._PDArray[did] = tmppd;
			tmpf = NULL;
			tmpd = NULL;
			//tmppd = NULL;
			dist._f[did][dist._fSize[did]-1] = 0;
			for (i=0;i<ARRAY_SIZE;i++){
				dist._DArray[did][(dist._fSize[did]-1)*ARRAY_SIZE+i] = dist._CDArray[did][seg->fid*ARRAY_SIZE+i];
				//for (j=0;j<ARRAY_SIZE;j++)
				//	dist._PDArray[did][(dist._fSize[did]-1)*ARRAY_SIZE*ARRAY_SIZE+i*ARRAY_SIZE+j] = dist._CPDArray[did][seg->fid*ARRAY_SIZE*ARRAY_SIZE+i*ARRAY_SIZE+j];
			}
			if(seg->fid == 1)
				fprintf(stderr, "number of unique chunks of file 0: %d\n",cnt);
			//Re-set bitmap for new file
			memset(dist._bitmap[did],0,sizeof(uint8_t)*134217728/8);
		}
	//	printf("the value of seg_unique is %d\n", seg->unique);
		if(seg->unique == 1){
			//Update f for current file
			dist._f[did][dist._fSize[did]-1]++;
			sbuf[cnt++] = seg;
		} else {
			//Update DArray and RArray
			int inner_dup = 1;
			//Should count only unique duplicate chunks for each file!!!
			//if(inner_dup == 0) {
				//Check if this duplicate chunks has been encountered in the
				//same file before

				if (((dist._bitmap[did][seg->id/8] & (1 << seg->id%8)) >> seg->id%8) == 0) {
					//pthread_mutex_lock(&map_lock);
				pthread_mutex_lock(dist._map_lock);
				void* dptr = kcdbget(dist._map,(char*)&seg->id, sizeof(uint64_t),&size);
				pthread_mutex_unlock(dist._map_lock);
				//void* dptr = leveldb_get(dist._lmap,dist._roptions,(const char*)&seg->id,sizeof(uint64_t),&size,&err);
				//pthread_mutex_unlock(&map_lock);
				//Set bit for this chunk as 1 if it's encountered for the first
				//time
				
				dist._bitmap[did][seg->id/8] += 1 << seg->id%8;
				if (dptr != NULL){
					inner_dup = 0;
					uint8_t disk = *(uint8_t*)dptr;
					dist.stat[did][disk]++;

					dist._DArray[did][(dist._fSize[did]-1)*ARRAY_SIZE+disk]++;
					dist._CDArray[did][seg->fid*ARRAY_SIZE+disk]++;
					kcfree(dptr);
					dptr = NULL;
					
					
/*
					pthread_mutex_lock(dist._csmap_lock);
					/void* sptr = kcdbget(dist._csmap,(char*)&seg->id,sizeof(uint64_t),&size);
					/pthread_mutex_unlock(dist._csmap_lock);
					if (sptr != NULL) {
						uint64_t stripe = *(uint64_t*)sptr;
						uint8_t str_disk = dist._sten[stripe].ddisk[STRIPE_SIZE-CODE_SIZE]; //query disk id of the first parity of the duplicate stripe
						dist._PDArray[did][(dist._fSize[did]-1)*ARRAY_SIZE*ARRAY_SIZE+disk*ARRAY_SIZE+str_disk]++;
						dist._CPDArray[did][seg->fid*ARRAY_SIZE*ARRAY_SIZE+disk*ARRAY_SIZE+str_disk]++;
						kcfree(sptr);
						sptr = NULL;
					}
				*/
					
				}
				}	
			//Check whether the duplicate
	//		printf("the number of cnt is %d\n", cnt); 
			if (inner_dup == 1){
			for(i=0;i<cnt;i++)
				if(sbuf[i]->id == seg->id){
					//check whether same duplicate <chunk id, file id> exists in
					//R list
					int r_dup = 0;
					if (sbuf[i]->fid == seg->fid)
						r_dup = 1;
					if (r_dup == 0){
						for (j=0;j<dist._sizeofR[did];j++){
							if (dist._RArray[did][j].block_id == i && dist._RArray[did][j].ab_file_id == seg->fid){
								r_dup = 1;
								break;
							}
						}
					}
					if (r_dup == 0){
					dist._sizeofR[did]++;
					struct ReferIntraSegment* tmp = (struct ReferIntraSegment*)malloc(sizeof(struct ReferIntraSegment)*dist._sizeofR[did]);
					if (tmp == NULL) perror("malloc");
					if (dist._sizeofR[did] > 1) {
						memcpy(tmp,dist._RArray[did],sizeof(struct ReferIntraSegment)*(dist._sizeofR[did]-1));
						free(dist._RArray[did]);
						dist._RArray[did] = NULL;
					}

					dist._RArray[did] = tmp;
					tmp = NULL;
					dist._RArray[did][dist._sizeofR[did]-1].block_id = i;
					dist._RArray[did][dist._sizeofR[did]-1].file_id = dist._fSize[did]-1;
					dist._RArray[did][dist._sizeofR[did]-1].ab_file_id = seg->fid;
					
					for (j=0;j<dist._sizeofR[did]-1;j++){
						if (dist._RArray[did][j].block_id > i) {
							for (k=dist._sizeofR[did]-2;k>=j;k--) {
								dist._RArray[did][k+1].block_id = dist._RArray[did][k].block_id;
								dist._RArray[did][k+1].file_id = dist._RArray[did][k].file_id;
								dist._RArray[did][k+1].ab_file_id = dist._RArray[did][k].ab_file_id;
							}
							dist._RArray[did][j].block_id = i;
							dist._RArray[did][j].file_id = dist._fSize[did] - 1;
							dist._RArray[did][j].ab_file_id = seg->fid;
							break;
						}
					}
					//inner_dup = 1;
					}
					break;
				}
			}
			

	 		Enqueue(dist._oq[did], seg);
		}
		
		if(cnt == SEG_BUF_SIZE){
		//Make decision on distribution of current buffered segments
		gettimeofday(&a,NULL);
		if (dist._method == B){
			Basline(sbuf,cnt,did);
		}
		else if (dist._method == E) {
			EDPBased(sbuf,cnt,did);
		} else if (dist._method == C) {
			CEDPBased(sbuf,cnt,did);
		} else if (dist._method == FE ) {
			fEDPBased(sbuf,cnt,did);
		} else if (dist._method == R ) {
                        RADBased(sbuf,cnt,did);
                }

		gettimeofday(&b,NULL);
		timersub(&b,&a,&c);
		fprintf(stderr,"DIST TIME: %ld.%06ld\n",c.tv_sec,c.tv_usec);
		fprintf(stderr,"File 7: %d\n",tmp_cnt);
		/*
		for (i = 0;i<cnt;i++) {
			sbuf[i]->disk = cnt%10;
			sbuf[i]->pdisk[0] = 10;
			sbuf[i]->pdisk[1] = 11;
			sbuf[i]->pid[0] = --dist._slog->segID;
			sbuf[i]->pid[1] = --dist._slog->segID;
			Enqueue(dist._oq, sbuf[i]);
		}
*/
			
			cnt=0;
			//Reset records
			dist._fSize[did] = 0;
			free(dist._f[did]);
			free(dist._DArray[did]);
			//free(dist._PDArray[did]);
			free(dist._RArray[did]);
			dist._RArray[did] = NULL;
			dist._f[did] = NULL;
			dist._DArray[did] = NULL;
			//dist._PDArray[did] = NULL;
			dist._sizeofR[did] = 0;
			//lfid=-1;
			cfid=-1;
		}

	}
	if(cnt > 0){
		//Make decision on distribution of current buffered segments
		if (dist._method == B){
		Basline(sbuf,cnt,did);
		}
		else if (dist._method == E) {
		EDPBased(sbuf,cnt,did);
		} else if (dist._method == C) {
		CEDPBased(sbuf,cnt,did);
		} else if (dist._method == FE ) {
			fEDPBased(sbuf,cnt,did);
		} else if (dist._method == R ) {
                        RADBased(sbuf,cnt,did);
                }


/*

for (i = 0;i<cnt;i++) {
                        sbuf[i]->disk = cnt%10;
                        sbuf[i]->pdisk[0] = 10;
                        sbuf[i]->pdisk[1] = 11;
                        sbuf[i]->pid[0] = --dist._slog->segID;
                        sbuf[i]->pid[1] = --dist._slog->segID;
			Enqueue(dist._oq, sbuf[i]);
                }
*/	
		cnt=0;
		//Reset records
		dist._fSize[did] = 0;
		free(dist._f[did]);
		free(dist._DArray[did]);
		//free(dist._PDArray[did]);
		free(dist._RArray[did]);
		dist._RArray[did] = NULL;
		dist._f[did] = NULL;
		dist._DArray[did] = NULL;
		//dist._PDArray[did] = NULL;
		dist._sizeofR[did] = 0;
	}
	/*
	for (j=0;j<=cfid;j++) {
		fprintf(stderr,"File %d: \n",j);
		for (i=0;i<ARRAY_SIZE;i++){
			fprintf(stderr,"%d,",dist._CDArray[did][j*ARRAY_SIZE+i]);
		}
		fprintf(stderr,"\n");
	}
	*/
	Enqueue(dist._oq[did],NULL);
	//dist._sb->Destroy();
	free(sbuf);
	return NULL;
}

static int start(Queue* iq, Queue* oq, int method, KCDB* sdmap, pthread_mutex_t* sdmap_lock, KCDB* csmap, pthread_mutex_t* csmap_lock, int serverID){

	pthread_mutex_lock(&dist._lock);
	int *i = (int*)malloc(sizeof(int));
	int j;
	fprintf(stderr,"enter distributor%d...\n",dist._num);
        for (*i=0;*i<NUM_THREADS;(*i)++)
	    if (dist._status[*i] == 0)
		break;

	assert(*i < NUM_THREADS);
	dist._iq[*i] = iq;
	dist._oq[*i] = oq;
	dist._srvID[*i] = serverID;
        dist._num++;
	dist._status[*i] = 1;

	//Accumulative duplicate array for each file in a upload window
	dist._CDArray[*i] = (int*)malloc(sizeof(int)*WINDOW_SIZE*ARRAY_SIZE);
	memset(dist._CDArray[*i],0,sizeof(int)*WINDOW_SIZE*ARRAY_SIZE);
	assert(dist._CDArray[*i] != NULL);
	//dist._CPDArray[*i] = (int*)malloc(sizeof(int)*WINDOW_SIZE*ARRAY_SIZE*ARRAY_SIZE);
	//memset(dist._CPDArray[*i],0,sizeof(int)*WINDOW_SIZE*ARRAY_SIZE*ARRAY_SIZE);
	//assert(dist._CPDArray[*i] != NULL);
	dist._fSize[*i] = 0;
	dist._sizeofR[*i] = 0;
	dist._bitmap[*i] = (uint8_t *) malloc(sizeof(uint8_t)*134217728/8);
	memset(dist._bitmap[*i],0,sizeof(uint8_t)*134217728/8);
	memset(dist.stat[*i],0,sizeof(int)*ARRAY_SIZE);


	if (dist._map == NULL || dist._csmap == NULL || dist._sten == NULL || dist._sen == NULL){
	dist._method = method;
	//Load Chunk to Slave Mapping
	dist._map = sdmap;//kcdbnew();
	dist._map_lock = sdmap_lock;

	//Load Chunk to Stripe Mapping
	dist._csmap = csmap;//kcdbnew();
	dist._csmap_lock = csmap_lock;
	
	/*
	leveldb_options_t *options = leveldb_options_create();
	dist._roptions = leveldb_readoptions_create();
	dist._woptions = leveldb_writeoptions_create();
	char *err = NULL;
	leveldb_options_set_create_if_missing(options,1);
	dist._lmap = leveldb_open(options,"meta/lsdmap",&err);
	if (err != NULL) {
		fprintf(stderr,"LEVELDB Open fail.\n");
		exit(1);
	}

	leveldb_free(err); err = NULL;
	*/

	int fd;
	FILE* fp;
	fp = fopen("random_bandwidths.dat", "r");
	assert(fp != NULL);
	for (j=0;j<ARRAY_SIZE;j++){
		fscanf(fp,"%d",&(dist._cost[j]));
		fprintf(stderr,"%d,",dist._cost[j]);
	}
	fprintf(stderr,"\n");
	fclose(fp);
	// Load Disk Log 
	fd = open("meta/stlog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, MAX_ENTRIES(sizeof(STEntry))/STRIPE_SIZE));
	dist._sten = MMAP_FD(fd, MAX_ENTRIES(sizeof(STEntry)/STRIPE_SIZE));
	dist._stlog = (StripeLog*) dist._sten;
	close(fd);	

	fd = open("meta/slog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, MAX_ENTRIES(sizeof(SMEntry))));
	dist._sen = MMAP_FD(fd, MAX_ENTRIES(sizeof(SMEntry)));
	dist._slog = (SegmentLog*) &dist._sen[MAX_ENTRIES(1)-1];
	if (dist._slog->segID == 0) dist._slog->segID = MAX_ENTRIES(1)-2;
	close(fd);	
	}

	int ret = pthread_create(&dist._tid[*i], NULL, process, i );
	pthread_mutex_unlock(&dist._lock);

	return ret;
}

static int stop(int serverID){

	pthread_mutex_lock(&dist._lock);
	int i;
	for (i=0;i<NUM_THREADS;i++)
	    if (dist._srvID[i] == serverID)
		break;
	assert(i<NUM_THREADS);
	int ret = pthread_join(dist._tid[i],NULL);
	dist._status[i] = 0;
	dist._num--;
	dist._srvID[i] = -1;
	free(dist._CDArray[i]);
	//free(dist._CPDArray[i]);
	free(dist._bitmap[i]);
	pthread_mutex_unlock(&dist._lock);
	
	if (dist._num == 0){
	//kcdbdumpsnap(dist._map, "meta/sdmap");
	if (!kcdbsync(dist._map,1,NULL,NULL)) 
	   fprintf(stderr,"map sync/dump error: %s\n",kcecodename(kcdbecode(dist._map)));
	/*
	uint64_t test = 9,size;
	void* dptr = kcdbget(dist._map,(const char*)&test,sizeof(uint64_t),&size);
	if (dptr != NULL) fprintf(stderr,"Chunk 9 on node %d\n",*(uint8_t*)dptr);
	*/
	//kcdbdel(dist._map);
	//kcdbdumpsnap(dist._csmap, "meta/csmap");
	if (!kcdbsync(dist._csmap,1,NULL,NULL))
	    fprintf(stderr,"csmap sync/dump error: %s\n",kcecodename(kcdbecode(dist._csmap)));
	//kcdbdel(dist._csmap);
//leveldb_close(dist._lmap);
	munmap(dist._sten,MAX_ENTRIES(sizeof(STEntry))/STRIPE_SIZE);
	munmap(dist._sen,MAX_ENTRIES(sizeof(SMEntry)));
	dist._sten = NULL;
	dist._sen = NULL;
	}
	printf("Distributor exit%d...\n", dist._num);

	return ret;
}

static Distributor dist = {
	._num = 0,
	._status = {0,0,0,0,0,0,0,0,0,0},
	.start = start,
	.stop = stop,
};

Distributor* GetDistributor(){
	return &dist;
}
