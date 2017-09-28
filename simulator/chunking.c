/**
 * @file 	chunking.c
 * @brief	Program that generates metadata file for further processing
 * @author	Ng Chun Ho, Xu Min
 */
#include <inttypes.h> 
#include <sys/types.h>
#include "chunking.h"
#include "include/libhashfile.h"
//#include "include/liblog.h"

#define MAXLINE	4096

static Chunker chk;

/**
 * Main loop that chunks the input files
 */
static void* process(void* ptr){
	int j;
	struct timeval a,b,c,d;
	timersub(&d,&d,&d);
	for(j=chk._start;j<chk._end;j++){
	int ifd = open(chk._list[j],O_RDONLY);
	//printf("Segment size: %d\n",sizeof(Segment));
#ifdef WITH_REAL_DATA
	// RABIN FINGERPRINT
	uint64_t size = lseek(ifd, 0, SEEK_END);
	uint8_t * data = MMAP_FD_RO(ifd, size);
	Queue* eq = NewQueue();
	Queue* rfq = NewQueue();
	
	gettimeofday(&a,NULL);
	RabinService* rs = GetRabinService();
	FpService* fs = GetFpService();
	rs->start(data,size,chk._vsize,j-chk._start,rfq);
	fs->start(rfq,eq);

	Segment* seg;
	while ((seg = (Segment*)Dequeue(eq)) != NULL) {
		Enqueue(chk._oq,seg);
	}

	rs->stop();
	fs->stop();
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	timeradd(&c,&d,&d);
	free(rfq);
	free(eq);
	munmap(data, size);
	chk._vsize += size;

#else

#ifdef SYNTHETIC
	uint64_t offset = 0;
	uint64_t size, i;
	uint8_t * fps = malloc(MAX_SEG_CHUNKS * FP_SIZE);
	Segment * seg; 
	while ((size = read(ifd, fps, MAX_SEG_CHUNKS * FP_SIZE)) > 0) {
		while((seg = malloc(sizeof(Segment))) == NULL) ;
		seg->offset = offset;
		seg->voffset = chk._vsize;
		seg->chunks = size / FP_SIZE;
		seg->len = seg->chunks * AVG_CHUNK_SIZE;
		seg->fid = j-chk._start;
	
		for (i = 0; i < seg->chunks; i++) {
			memcpy(seg->en[i].fp, fps + i * FP_SIZE, FP_SIZE);
			seg->en[i].pos = i * AVG_CHUNK_SIZE;
			seg->en[i].len = AVG_CHUNK_SIZE;
			seg->en[i].ref = 0;
		}
		SHA1(fps, size, seg->fp);
		offset += AVG_SEG_SIZE;
		chk._vsize += AVG_SEG_SIZE;
		Enqueue(chk._oq,seg);
	}
	free(fps);
#endif

#ifdef FSL
	/* Read from pre-made metadata file */
	/*
	uint64_t offset = 0;
	uint64_t size, i;
	uint8_t* fps = malloc(sizeof(Segment));
	Segment* seg; 
	while ((size = read(ifd, fps, sizeof(Segment))) > 0) {
		while((seg = malloc(sizeof(Segment))) == NULL) ;
		memcpy(seg,fps,sizeof(Segment));
		//if (seg->fid < 400 && seg->fid >= 300){
		fprintf(stderr,"segment of file %ld at %ld of length %d\n",seg->fid,seg->offset,seg->len);
		Enqueue(chk._oq,seg);
		//}
	}
	free(fps);	
	*/
	fprintf(stderr,"start FSL chunking...\n");
	gettimeofday(&a,NULL);
	char buf[MAXLINE];
	struct hashfile_handle *handle;
	const struct chunk_info *ci;
	uint64_t chunk_count;
	time_t scan_start_time;
	int ret;

	handle = hashfile_open(chk._list[j]);
	if (!handle) {
		fprintf(stderr, "Error opening hash file: %d!", errno);
		return NULL;
	}

	// Print some information about the hash file
	scan_start_time = hashfile_start_time(handle);
	printf("Collected at [%s] on %s",
			hashfile_sysid(handle),
			ctime(&scan_start_time));

	ret = hashfile_chunking_method_str(handle, buf, MAXLINE);
	if (ret < 0) {
		fprintf(stderr, "Unrecognized chunking method: %d!", errno);
		return NULL;
	}

	printf("Chunking method: %s", buf);

	ret = hashfile_hashing_method_str(handle, buf, MAXLINE);
	if (ret < 0) {
		fprintf(stderr, "Unrecognized hashing method: %d!", errno);
		return NULL;
	}

	printf("Hashing method: %s\n", buf);

	// Go over the files in a hashfile
	printf("== List of files and hashes ==\n");
	uint64_t offset;
	uint64_t i, fcnt=0;
	uint64_t seg_len;
	uint32_t clen[MAX_SEG_CHUNKS];
	uint8_t * fps = malloc(MAX_SEG_CHUNKS * FP_SIZE);
	Segment* seg;
	while (1) {
		ret = hashfile_next_file(handle);
		if (ret < 0) {
			fprintf(stderr,
				"Cannot get next file from a hashfile: %d!\n",
				errno);
			return NULL;
		}

		// exit the loop if it was the last file
		if (ret == 0)
			break;

		//printf("File path: %s\n", hashfile_curfile_path(handle));
		//printf("File %d size: %"PRIu64 " B\n", fcnt, hashfile_curfile_size(handle));
		//printf("Chunks number: %" PRIu64 "\n", hashfile_curfile_numchunks(handle));
		if (hashfile_curfile_size(handle) >= 32768){
		// Go over the chunks in the current file 
		chunk_count = 0;
		offset = 0;
		seg_len = 0;
		while (1) {
			ci = hashfile_next_chunk(handle);
			if (!ci) // exit the loop if it was the last chunk 
				break;

			chunk_count++;

			//print_chunk_hash(chunk_count, ci->hash, hashfile_hash_size(handle) / 8);
			//Process chunk
			SHA1(ci->hash,hashfile_hash_size(handle)/8,fps+((chunk_count-1)%MAX_SEG_CHUNKS)*FP_SIZE);
			clen[(chunk_count-1)%MAX_SEG_CHUNKS] = ci->size;
			if ( chunk_count%MAX_SEG_CHUNKS == 0 ){
				while((seg = malloc(sizeof(Segment))) == NULL) ;
				seg->offset = offset;
				seg->voffset = chk._vsize;
				seg->chunks = MAX_SEG_CHUNKS;
				//seg->len = seg_len; //seg->chunks * AVG_CHUNK_SIZE;
				seg->fid = fcnt;
	
				seg_len = 0;
				for (i = 0; i < seg->chunks; i++) {
					memcpy(seg->en[i].fp, fps + i * FP_SIZE, FP_SIZE);
					seg->en[i].pos = seg_len;
					seg->en[i].len = clen[i];
					seg_len += clen[i];
					seg->en[i].ref = 0;
				}
				SHA1(fps, FP_SIZE*MAX_SEG_CHUNKS, seg->fp);
				offset += seg_len;
				chk._vsize += seg_len;
				seg->len = seg_len;
				//fprintf(stderr,"File %d at %ld with length %d\n",seg->fid,seg->offset,seg->len);
				Enqueue(chk._oq,seg);	
			}
		}
		if ( chunk_count%MAX_SEG_CHUNKS != 0) {
			while((seg = malloc(sizeof(Segment))) == NULL) ;
			seg->offset = offset;
			seg->voffset = chk._vsize;
			seg->chunks = chunk_count%MAX_SEG_CHUNKS;
				//seg->len = seg_len; //seg->chunks * AVG_CHUNK_SIZE;
			seg->fid = fcnt;
	
			seg_len = 0;
			for (i = 0; i < seg->chunks; i++) {
				memcpy(seg->en[i].fp, fps + i * FP_SIZE, FP_SIZE);
				seg->en[i].pos = seg_len;
				seg->en[i].len = clen[i];
				seg_len += clen[i];
				seg->en[i].ref = 0;
			}
			SHA1(fps, FP_SIZE*seg->chunks, seg->fp);
			offset += seg_len;
			chk._vsize += seg_len;
			seg->len = seg_len;
			//fprintf(stderr,"File %d at %ld with length %d\n",seg->fid,seg->offset,seg->len);
			Enqueue(chk._oq,seg);
		}
		fcnt++;
		}
	}
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	timeradd(&c,&d,&d);

	hashfile_close(handle);
#endif

#endif
	close(ifd);
	}
	Enqueue(chk._oq, NULL);
	fprintf(stderr,"CHK Service %ld.%06ld\n",d.tv_sec,d.tv_usec);
	return NULL;
}



/**
 * Implements Chunker->start
 * @param _list		list of filenames
 * @param _start	index of the first file to upload in the list
 * @param _end		index of the last file to upload in the list
 * @param oq		Queue for outgoing metadata of chunks
 * @return		0 on success, -1 on failure
 */
static int start(char** _list, int _start, int _end, Queue* oq){
	chk._oq = oq;
	//chk._if = _if;
	//chk._df = _df;
	chk._cnt = 0;
	chk._list = _list;
	chk._start = _start;
	chk._end = _end;
	chk._vsize = 0;

	int ret = pthread_create(&chk._tid, NULL, process, NULL);

	return ret;
}

/**
 * Implements Chunker->stop
 * @return 	0 on success, -1 on failure
 */
static int stop(){
	int ret = pthread_join(chk._tid,NULL);
	return ret;
}

static Chunker chk = {
	.start = start,
	.stop = stop,
};

Chunker* GetChunker(){
	return &chk;
}


