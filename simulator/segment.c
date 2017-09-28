#include "segment.h"

/*
 * Implementation of online parity declustering mapping
 * inspired by paper: Efficient Data Mappings
 * for Parity-Declustered Data Layouts
 */

static SegBuf sb;

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
 * Return the stripe consisting array of disk ids at the rank-th row of a
 * segment
 */
static inline int invloc(int rank, int offset){
	int i;

	for(i=sb._G;i>=1;i--){
		int l = i;
		while(b(l,i) <= rank)
			l = l+1;
		//stripe[i] = l-1;
		if(offset == i-1) 
			return l-1;
		rank = rank - b(l-1,i);
	}
	return 0;
}

static void Create(int C, int G, int P){
	sb._C = C;
	sb._G = G;
	sb._P = P;

	sb._size = b(C,G)*G*G;
	sb._disk = (int*)malloc(sizeof(int)*C);
	int i;
	for(i=0;i<C;i++)
		sb._disk[i] = b(C,G)*G*G/C;
}

static int GetDisk(int index){
	int offset = index%(sb._G-sb._P);
	int rank = index/(sb._G-sb._P)%(b(sb._C,sb._G));
	int section = index/(sb._G-sb._P)/(b(sb._C,sb._G));
	offset = (section+offset)>=(sb._G-sb._P)?offset+sb._P:offset;
	int disk = invloc(rank,offset);

	return disk;
}

static int GetParity(int index){
	int rank = index/(sb._G-sb._P)%(b(sb._C,sb._G));
	int section = index/(sb._G-sb._P)/(b(sb._C,sb._G));
	int offset = sb._G-section-1;
	int disk = invloc(rank,offset);
	
	return disk;
}

static void Insert(int disk){
	sb._disk[disk]--;
}

static int GetQuota(int disk){
	return sb._disk[disk];
}

static void Reset(){
	int i;
    for(i=0;i<sb._C;i++)
        sb._disk[i] = b(sb._C,sb._G)*sb._G*sb._G/sb._C;
}

static void Destroy(){
	free(sb._disk);
	sb._disk = NULL;
}

static SegBuf sb = {
	.Create = Create,
	.GetDisk = GetDisk,
	.GetParity = GetParity,
	.Insert = Insert,
	.GetQuota = GetQuota,
	.Reset = Reset,
	.Destroy = Destroy,
};

SegBuf* GetSegBuf(){
	return &sb;
}
