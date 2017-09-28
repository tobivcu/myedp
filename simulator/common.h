#ifndef _COMMON_H_
#define _COMMON_H_

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <endian.h>
#include <assert.h>


#define NUM_THREADS 10

#define DEDUP

#define SIMULATION

#define WORK_DIR "/home/tong/Documents/edp/edp-1.0.0/simulator" // "/home/tong/Documents/edp/edp-1.0.0/simulator" // "/home/tong/Documents/edp/edp-1.0.0/simulator" // "/home/tong/Documents/edp/edp-1.0.0/simulator" // "/home/tong/Documents/edp/edp-1.0.0/simulator" // "" // The directory where the executable is executed

//#define FSL
//#define SYNTHETIC

#define SEG_SIZE 192ULL //decided by size of struct Segment

#define BUF_SIZE (SEG_SIZE*16) //size of recv/send buffer for socket communication

#define FP_SIZE 20

#define BLOCK_SIZE 4096ULL

#define STRIPE_SIZE 14 

#define ARRAY_SIZE 16 

#define CODE_SIZE 2

#define SEG_BUF_SIZE 20160 // 7722 // 5280 // 5280 // 10920 // 7722 // 5280 // 5280 // 3465 // 2160 // 2160 // 16800 // 16800// 7722 // 7722 // 5280 // 3465 // 5280 // 3465 // 3465 // 2160 // 2160 // 2160 // 16800//140//16800//5426400///44767800//

#ifdef FSL
#define WINDOW_SIZE 100000
#else
#define WINDOW_SIZE 30000
#endif

/**
 *  Max number of entries in the log. Can set to a very large value
 *  since revdedup use ftruncate() to create space in the log file, and
 *  most file systems treats it as a sparse file
 */
#define MAX_ENTRIES(x) ((1024ULL << 18) * x)

#define WITH_REAL_DATA
/**
 *  Max number of images. Can set to larger values.
 */
#define INST_MAX(x) (131072 * x)
#ifdef WITH_REAL_DATA
#define CHUNK_SHIFT 0           /*!< 0 to switch to fixed size chunking */
#define DISABLE_COMPRESSION /*!< Comment to enable compression */
#define AVG_CHUNK_SIZE 4096ULL  /*!< Average chunk size */
#define AVG_SEG_BLOCKS 1     /*!< Segment size = AVG_SEG_BLOCKS * BLOCK_SIZE */
#else
#define CHUNK_SHIFT 0           /*!< 0 to switch to fixed size chunking */
#define DISABLE_COMPRESSION     /*!< Comment to enable compression */
#define AVG_CHUNK_SIZE 4096ULL  /*!< Average chunk size */
//#define AVG_SEG_BLOCKS 1024       /*!< Segment size = AVG_SEG_BLOCKS * BLOCK_SIZE */
////#define AVG_SEG_BLOCKS 1
#define AVG_SEG_BLOCKS 1
#endif

#define MIN_CHUNK_SIZE (AVG_CHUNK_SIZE >> CHUNK_SHIFT)
#define MAX_CHUNK_SIZE (AVG_CHUNK_SIZE << CHUNK_SHIFT)
#define ZERO_SIZE MAX_CHUNK_SIZE

#define AVG_SEG_SIZE (AVG_SEG_BLOCKS * BLOCK_SIZE)
#define MIN_SEG_SIZE (AVG_SEG_SIZE >> CHUNK_SHIFT)
#define MAX_SEG_SIZE (AVG_SEG_SIZE << CHUNK_SHIFT)

#define MAX_SEG_CHUNKS (MAX_SEG_SIZE / MAX_CHUNK_SIZE)

#define SEEK_ON_WRITE

#define ZERO_FP "\x1c\xea\xf7\x3d\xf4\x0e\x53\x1d\xf3\xbf\xb2\x6b\x4f\xb7\xcd\x95\xfb\x7b\xff\x1d"

/*
typedef struct {
	uint64_t id;
	uint8_t disk;
} segmsg;

bool_t xdr_segmsg(XDR *xdr,segmsg* smsg){
	return xdr_uint64_t(xdr,&smsg->id) && xdr_uint8_t(xdr,&smsg->disk);
}
*/
typedef struct _disk {
    int id;
    struct _disk* next;
} Disk;

typedef struct {
	int isSet;
	int lp;
} MDLOG;

typedef struct {
    int seg_id;
    int disk_id;
} DBEntry;

/** Chunk metadata on disk */
typedef struct {
    uint8_t fp[FP_SIZE];        /*!< Fingerprint of the chunk */
    uint32_t ref;               /*!< Reference count */
    uint32_t pos;               /*!< Position in segment */
    uint32_t len;               /*!< Length */
} CMEntry;

typedef struct {
    int id;
} StripeLog;

/** Image metadata on disk */
typedef struct {
	uint64_t versions;			/*!< Number of versions */
	uint64_t deleted;			/*!< Number of deleted versions */
	uint64_t old;				/*!< Number of versions that is reverse deduped */
	uint64_t recent;			/*!< Number of versions that is considered new */
	struct {
		uint64_t size;			/*!< Size of image */
		uint64_t space;			/*!< Space taken by unique segments of that image */
		uint64_t csize;			/*!< Compressed size of all segments */
		uint64_t cspace;		/*!< Space taken by unique segment after compression */
	} vers[255];
} IMEntry;

typedef struct {
	uint64_t id;
	uint64_t stat[ARRAY_SIZE];
} FEntry;

typedef struct {
	uint64_t id;
} FLog;

/**
 * Direct entry in the image recipe. It directly references the segment by
 * their ID. In restoration, loop through the recipe file and get the segments
 * from the direct entries.
 */
typedef struct {
	uint64_t index;			/*!< Starting byte of the segment in that image */
	uint64_t id;			/*!< Referenced segment ID */
	uint8_t disk; 			/*!< Disk ID*/
	uint32_t len; 
	uint64_t stid;
} Direct;

/** Segment metadata on disk */
typedef struct {
    uint8_t fp[FP_SIZE];        /*!< Fingerprint of the segment */
    uint32_t ref;               /*!< Reference count */
    uint8_t disk; 		/*!< Id of Disk where this segment resides*/
    //Disk* head; 	    	/*!< IDs of occupied disks*/
    //Disk* tail; 		/**/
    //int dlen; 		    	/*!< Length of list of unavailable disks */
    //uint64_t bucket;            /*!< Bucket ID */
    //uint32_t pos;               /*!< Position in bucket */
    //uint32_t len;               /*!< Length (after compression) */
    //uint64_t cid;               /*!< Respective starting chunk ID */
    //uint32_t chunks;            /*!< Number of chunks in segment */
    //uint16_t compressed;        /*!< Bit indicating if this chunk is unique */
    //uint16_t removed;           /*!< Removed size (not used) */
} SMEntry;

/** Segment metadata in memory */
typedef struct {
    uint64_t offset;        /*!< Offset in image */
    uint64_t id;            /*!< Global segment ID */
    uint8_t disk; 	    /*!< Disk ID of This Segment*/
    uint32_t len;           /*!< Length of chunk (before compression) */
	uint64_t fid;           /*!< File ID of This Segment*/
    //Disk* head; 	    /*!< IDs of occupied disks*/
    //Disk* tail;             /**/
    //int dlen; 		    /*!< Length of list of unavailable disks */
    uint64_t fsize;           /*!< Position in Bucket */
	uint64_t stid;    		/*!< Stripe ID of the segment */
	uint8_t stlen;   		/*!< Length of stripe to which this segment belongs */
	uint8_t pdisk[CODE_SIZE];
	uint64_t pid[CODE_SIZE];
	//uint8_t stlead;         /*!< Disk ID of leading node for encoding/decoding */
    uint64_t cid;           /*!< Respective starting chunk ID */
    uint16_t chunks;        /*!< Number of chunks in segment */
    uint8_t unique;         /*!< Bit indicating if this chunk is unique */
    //uint8_t compressed;     /*!< Bit indicating if this chunk is compressed */
    uint8_t fp[FP_SIZE];    /*!< Fingerprint of the segment */
	//pthread_mutex_t mutex; 
	uint64_t voffset;       /*!< virtual offset in a large flat file consisting of all files of a directory */
    uint64_t clen;          /*!< Length of chunk (after compression) */
    char * data;         /*!< Pointer to data */
    uint8_t * cdata;        /*!< Pointer to compressed data */
    CMEntry en[MAX_SEG_CHUNKS];     /*!< Respective chunk entries */

	uint64_t next_offset;
	int32_t next_len;
	int32_t next_fid;
	uint64_t next_voffset;
} Segment;

/**
 *  * Segment log metadata. It is placed in the first entry space
 *   * in the log file. So the first segment ID to be distributed is 1, not 0.
 *    */
typedef struct {
    uint64_t segID;         /*!< Global counter of segment ID distributed */
} SegmentLog;

typedef struct {
    uint64_t id;
    uint8_t len;
	uint64_t sid[STRIPE_SIZE];
	uint32_t size[STRIPE_SIZE];
    uint8_t ddisk[STRIPE_SIZE];
	//uint8_t pdisk[CODE_SIZE];
} STEntry;

typedef struct {
	uint64_t id;
	uint32_t size;
	uint16_t k;
	uint8_t m;
	uint8_t w;
	uint32_t packetsize;
	uint32_t buffersize;
	uint32_t tech;
	uint32_t readins;
	char codes[16];
} StripeMeta;

enum {
	B,
	E,
	C,
	FE,
	R
};

/** mmap a file */
#define MMAP_FD(fd, size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
/** mmap a file in read-only mode */
#define MMAP_FD_RO(fd, size) mmap(0, size, PROT_READ, MAP_SHARED, fd, 0)
/** allocate a piece of memory (instead of malloc) */
#define MMAP_MM(size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0)
/** mmap a file which subsequent changes would not affect the data in the file */
#define MMAP_FD_PV(fd, size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)

#define likely(x)    __builtin_expect (!!(x), 1)
#define unlikely(x)  __builtin_expect (!!(x), 0)

#define TIMERSTART(x) gettimeofday(&x, NULL)
#define TIMERSTOP(x) do { struct timeval a = x, b; gettimeofday(&b, NULL); timersub(&b, &a, &x); } while (0)


#endif
