/**
 * @file	image.h
 * @brief	Image Service Definition
 * @author	Ng Chun Ho
 */

#ifndef IMAGE_H_
#define IMAGE_H_

#include "common.h"
#include <queue.h>
#include <kclangc.h>
/**
 * Image service control
 */
typedef struct {
	int _num;
	int _status[NUM_THREADS];
	int _srvID[NUM_THREADS];
	pthread_mutex_t _lock;
	pthread_t _tid[NUM_THREADS];			/*!< Thread ID for processing segments */
	Queue * _iq[NUM_THREADS];			/*!< Queue for incoming segments */
	Queue * _oq[NUM_THREADS];			/*!< Queue for outgoing segments */
	int _fd[NUM_THREADS];				/*!< File descriptor of image recipe */

	IMEntry * _en;			/*!< Image entries */
	FLog * _flog;
	uint32_t _ins;			/*!< Image ID */
	uint32_t _ver;			/*!< Image version */

	/**
	 * Starts the image service
	 * @param iq		Queue for incoming segments
	 * @param oq		Queue for outgoing segments
	 * @param imageID	Image ID
	 * @return			0 if successful, -1 otherwise
	 */
	int (*start)(Queue * iq, Queue * oq, int serverID);
	/**
	 * Stops the image service
	 * @return			0 if successful, -1 otherwise
	 */
	int (*stop)(int serverID);
} ImageService;

ImageService* GetImageService();

#endif /* IMAGE_H_ */
