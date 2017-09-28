#include "common.h"
#include <kclangc.h>

int cost_array[ARRAY_SIZE];

int main(int argc, char** argv)
{
	if (argc != 4) {
		printf("\nUsage:\n%s <number of instances>  <log file> <snapshot name>\n",argv[0]);
		return 0;
	}
	int ins = atoi(argv[1]);
	int i,j;
	int fd,size,chks;
	uint64_t stat[ARRAY_SIZE];
	char path[64];
	KCDB* map = NULL;
	//KCDB* csmap = NULL;
	//////////////////////HEHE
	//struct stat sdstat,sdtmp;
	//Bitmap to filter out duplicate chunk
	uint8_t* bitmap = (uint8_t*)malloc(sizeof(uint8_t)*134217728/4);
	//Load Chunk To Slave Map
	//if (map == NULL) {
		printf("load sdmap\n");
		map = kcdbnew();
		kcdbopen(map,"meta/index/sdmap.kch#bnum=10M#msiz=64M",KCOREADER|KCONOLOCK);
		//kcdbloadsnap(map,"meta/sdmap");
	//}
	/*
	csmap = kcdbnew();
	kcdbopen(csmap,"meta/index/csmap.kch#bnum=10M#msiz=64M",KCOREADER|KCONOLOCK);
	fd = open("meta/stlog",O_RDONLY);
	STEntry* sten = MMAP_FD_RO(fd,MAX_ENTRIES(sizeof(STEntry))/STRIPE_SIZE);
	close(fd);
	*/
	/*
	if (stat("meta/sdmap",&sdtmp) == -1) {
		perror("meta/sdmap");
		exit(1);
	}
	if (sdtmp.st_mtime != sdstat.st_mtime) {
		kcdbloadsnap(map,"meta/sdmap");
		sdstat.st_mtime = sdtmp.st_mtime;
	}
	*/
	FILE* fp;
	fp = fopen("random_bandwidths.dat", "r");
	assert(fp != NULL);
	for (j=0;j<ARRAY_SIZE;j++){
	    fscanf(fp,"%d",&(cost_array[j]));
	}
	fclose(fp);
	fp = fopen(argv[2],"a+");

	for (i=0;i<ins;i++) {
		//Load Recipe Data
		sprintf(path,"image/%d/%d-0",i/256,i);
		fd = open(path,O_RDONLY);
		if ( fd == -1) {
			//fprintf(stderr,"image %s\n",path);
			continue;
		}
		size = lseek(fd,0,SEEK_END);
		if (size == -1) perror("lseek");
		chks = size/sizeof(Direct);
		Direct* dir = (Direct*)MMAP_FD_RO(fd,size);
		close(fd);

		memset(bitmap,0,sizeof(uint8_t)*134217728/4);
		memset(stat,0,sizeof(uint64_t)*ARRAY_SIZE);
		uint64_t dbsize;
		for (j=0;j<chks;j++) {
			if (((bitmap[dir[j].id/8] & ( 1 << dir[j].id%8 )) >> dir[j].id%8) == 0 ) {
			//Required chunk has not been encountered before
				bitmap[dir[j].id/8] += 1<<dir[j].id%8;
				void* dptr = kcdbget(map,(const char*)&(dir[j].id),sizeof(uint64_t),&dbsize);
				if (dptr != NULL) {
			        uint8_t disk = *(uint8_t*)dptr;
					stat[disk]++;
					/*
			        if (disk != 0)
			            stat[disk]++; //chunk number based stat
			            //stat[*(uint8_t*)dptr]+=dir[j].len; //chunk size based stat
			        else {
			            void* sptr = kcdbget(csmap, (char*)&(dir[j].id),sizeof(uint64_t),&dbsize);
			            if (sptr != NULL){
			                uint64_t stripe = *(uint64_t*)sptr;
							for (k=0;k<(STRIPE_SIZE-CODE_SIZE+1);k++){
								if (sten[stripe].ddisk[k] != disk && ((bitmap[sten[stripe].sid[k]/8] & ( 1 << sten[stripe].sid[k]%8 )) >> sten[stripe].sid[k]%8) == 0){
									bitmap[sten[stripe].sid[k]/8] += 1<<sten[stripe].sid[k]%8;
			                		stat[sten[stripe].ddisk[k]]++;
									//fprintf(stderr,"download %ld from %d\n",sten[stripe].sid[k], sten[stripe].ddisk[k]);
								}
							}
			            } else {
			                fprintf(stderr,"stripe not found.\n");
			            }
			        }
					*/
			    }
			}
		}
		//for (j=0;j<ARRAY_SIZE;j++)
		//	printf("Node %ld. %lu segments\n",j,stat[j]);
#ifndef HETERO
	int sum = 0;
	//int max = 0;
		double max = 0.0;
		double denom = 0.0;
		fprintf(fp,"%s:\n",argv[3]);
		for (j=0;j<ARRAY_SIZE;j++){
			sum += stat[j];
			denom += (double)cost_array[j];
			if ((double)stat[j]/cost_array[j] > max) max = (double)stat[j]/cost_array[j];
			//if (stat[j] > max) max = stat[j];
			fprintf(fp,"%ld/%d, ",stat[j],cost_array[j]);
		}
		fprintf(fp,"\n");
		//int even = (sum+ARRAY_SIZE-2)/(ARRAY_SIZE-1);
		//int even = (sum+ARRAY_SIZE-1)/ARRAY_SIZE;
		double even = (double)sum/denom;
		double ratio = (double)(max-even)/max;
		fprintf(fp,"Ratio: %06f\nMax: %06f\n",ratio,max);
#endif
		munmap(dir,size);
	}
	fclose(fp);

	free(bitmap);
	kcdbclose(map);
	kcdbdel(map);
	/*
	kcdbclose(csmap);
	kcdbdel(csmap);
	munmap(sten, MAX_ENTRIES(sizeof(STEntry))/STRIPE_SIZE);
	*/
	return 0;
}
