#ifndef _SEGMENT_H_
#define _SEGMENT_H_

#include <limits.h>
#include <stdlib.h>

typedef struct {
	int _C;
	int _G;
	int _P;
	unsigned long _size;
	int* _disk;
	void (*Create)(int C, int G, int P);
	int (*GetDisk)(int index);	
	int (*GetParity)(int index);
	void (*Insert)();
	int (*GetQuota)(int disk);
	void (*Reset)();
	void (*Destroy)();
} SegBuf;

SegBuf* GetSegBuf();

#endif
