#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

char *filePath;
int numThreads = 5;
int PIPE_BUF = 4096;
struct thread_info *infoArr;

void *threadFunc(void *arg);

int main(int argc, char **argv)
{
	if (argc != 2)
	{
		printf("Usage: %s file-path\n", argv[0]);
	       return 1;	
	}

	filePath = argv[1];

	pthread_attr_t attr;

	int initReturn = pthread_attr_init(&attr);

	if (initReturn != 0)
	{
		printf("Failed to init thread attr with error %\n", initReturn);
		return 4;
	}


	pthread_t threads[numThreads];

	for (int i = 0; i < numThreads; i++)
	{
		int threadStartVal = pthread_create(&threads[i], &attr, threadFunc, (void *) i);
		if (threadStartVal != 0)
		{
			printf("failed to create thread %d with error %d\n", i, threadStartVal);
			return 6;
		}
	}

	int fileSize = 0;

	for (int i = 0; i < numThreads; i++)
	{
		void * result;
		int joinResult = pthread_join(threads[i], &result);
		if (joinResult != 0)
		{
			printf("Joining thread %d failed with error code %d\n", i, joinResult);
		}
		else
		{
			printf("Thread %d wrote %d bytes\n", i, (int)result);
		}

		if ((int)result > 0)
		{
			fileSize += (int)result;
		}
	}

	int readFd = open(filePath, O_RDONLY);
	void *readBuffer = malloc(PIPE_BUF);

	for (int i = 0; i < fileSize;)
	{
		int readReturn = read(readFd, readBuffer, PIPE_BUF);
		if (readReturn <= 0)
		{
			printf("read failed");
			return 7;
		}

		printf("read %d bytes from file offset %d\n", readReturn, i);

		char threadChar = ((char *)readBuffer)[i];
		for (int j = 1; j < readReturn; j++)
		{
			if (((char*)readBuffer)[j] != threadChar)
			{
				printf("found an inconsistency");
			}
		}
		i += readReturn;
	}
}


volatile int fd = 0;

void *threadFunc(void *arg)
{
	int threadNumber = (int)arg;

	void *buffer = malloc(PIPE_BUF);

	if (!buffer)
	{
		printf("Couldn't allcate IO buffer\n");
		return (void *)-2;
	}

	for (int i = 0; i < PIPE_BUF; i++)
	{
		*((char *)buffer + i) = ('0' + threadNumber);
	}

	while (fd == 0)
	{
		int openAttempt = open(filePath, O_WRONLY | O_CREAT | O_DSYNC | O_SYNC | O_APPEND); 
		if (openAttempt > 0)
		{
			fd = openAttempt;
		}
	}

	if (fd <= 0)
	{
		printf("Couldn't open the specified path\n");
		return (void *)-3;
	}

	int amountWritten = 0;
	for (int i = 0; i < 5; i++)
	{
		int thisWrite = write(fd, buffer, PIPE_BUF);
		printf("wrote %d bytes out of %d\n", amountWritten, PIPE_BUF);
		if (thisWrite > 0)
		{
			amountWritten += thisWrite;
		}
	}

	free(buffer);

	return (void *)amountWritten;
}




