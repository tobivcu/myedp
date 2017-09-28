/**
 * @file	rabin.c
 * @brief	Rabin Chunking Service Implementation
 * @author	Ng Chun Ho
 */
#include "rabin.h"
#include "fingerprint.h"

#define LQ(x) (x & (lq - 1))				// lq-bit mask for matching
#define UQ(x) (x & (uq - 1))				// uq-bit mask for matching

static const uint64_t d = 587;				// base
static const uint64_t m = 32;				// window size
static const uint64_t mv = 0x87;			// Matching value
static const uint64_t lq = AVG_CHUNK_SIZE;	// average size of chunks
static const uint64_t uq = AVG_SEG_SIZE;	// average size of segments

static uint64_t _t[256];					// pre-computed (t_s * d^m) mod q

RabinService service;

/**
 * Creates a new segment
 * @param offset		Offset in the image processed
 * @param ptr			Pointer to data
 * @return				Created segment
 */
static inline Segment * newSegment(uint64_t offset, uint8_t * ptr) {
	Segment * seg = malloc(sizeof(Segment));
	memset(seg, 0, sizeof(Segment));
	seg->offset = offset;
	seg->len = 0;
	seg->chunks = 0;
	seg->data = (char*)ptr;
	seg->fid = service._fid;
	seg->voffset = service._vsize;
	return seg;
}

/**
 * Insert a chunk into segment
 * @param seg			Segment for insertion
 * @param pos			Starting position of chunk in the segment
 * @param len			Length of the chunk
 */
static inline void insertChunk(Segment * seg, uint32_t pos, uint32_t len) {
//		printf("Chunks:%ld\n",seg->chunks);
seg->en[seg->chunks].pos = pos;
	seg->en[seg->chunks].len = len;
	seg->en[seg->chunks].ref = 0;
	seg->len += len;
	seg->chunks++;
//	printf("Chunks:%ld\n",seg->chunks);
}

/**
 * Main loop for chunking  segments
 * @param ptr		useless
 */
void * process(void * ptr) {
	uint8_t * _b = service._data;
	uint64_t v = 0;

	uint64_t index = 0;
	uint64_t b_ptr = 0;
	uint64_t r_ptr = m;

	uint64_t i;
	for (i = 0; i < m; i++) {
		v = UQ((d * v + _b[i]));
	}

	Segment * seg = newSegment(index, _b + index);
	while (1) {
		v = UQ(((d * v) - _t[_b[r_ptr - m]] + _b[r_ptr]));
		r_ptr++;
		// Break when all data is processed
		if (unlikely(r_ptr == service._size)) {
			insertChunk(seg, b_ptr - index, r_ptr - b_ptr);
			b_ptr = r_ptr;
			Enqueue(service._q, seg);
			break;
		}
		// Only outputs segment when it is larger than MIN_SEG_SIZE
		if (seg->len + r_ptr - b_ptr >= MIN_SEG_SIZE) {
			// outputs segment if the current byte creates a segment cut
			// or segment length equals MAX_SEG_SIZE
			if (v == mv || unlikely(seg->len + r_ptr - b_ptr == MAX_SEG_SIZE)) {
				// A segment cut must be a chunk cut
				insertChunk(seg, b_ptr - index, r_ptr - b_ptr);
				b_ptr = r_ptr;
				//fprintf(stderr,"segment of file %ld at %ld(%ld) of len %d\n",seg->fid, seg->offset,seg->voffset, seg->len);
				Enqueue(service._q, seg);
				service._vsize = service._vsize - index + b_ptr;
				index = b_ptr;
				seg = newSegment(index, _b + index);
				continue;
			}
		}

		// Insert chunk into segment if current byte is a chunk cut
		// or chunk length equals MAX_CHUNK_SIZE
		if (unlikely(LQ(v) == mv) || unlikely((r_ptr - b_ptr) == MAX_CHUNK_SIZE)) {
			if ((r_ptr - b_ptr) < MIN_CHUNK_SIZE) {
				continue;
			}
			insertChunk(seg, b_ptr - index, r_ptr - b_ptr);
			b_ptr = r_ptr;
		}
	}
	Enqueue(service._q, NULL );
	return NULL ;
}

/**
 * Implements RabinService->start()
 */
int start(uint8_t * data, uint64_t size, uint64_t vsize, uint64_t fid, Queue * oq) {
	uint64_t _dm = 1;
	uint64_t i;
	for (i = 1; i <= m; i++) {
		_dm = UQ(_dm * d);
	}

	for (i = 0; i < 256; i++) {
		_t[i] = UQ(i * _dm);
	}

	service._data = data;
	service._size = size;
	service._vsize = vsize;
	service._fid = fid;

	service._q = oq;
	int ret = pthread_create(&service._tid, NULL, process, NULL );
	return ret;
}

/**
 * Implements RabinService->stop()
 */
int stop() {
	int ret = pthread_join(service._tid, NULL);
	return ret;
}

RabinService service = {
		.start = start,
		.stop = stop,
};


RabinService* GetRabinService() {
	return &service;
}
