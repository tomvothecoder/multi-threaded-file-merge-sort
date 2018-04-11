#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

// Shared variable
int thread_count;
pthread_mutex_t (&mutex);

void *merge(void* rank);

int main(int argc, char* argv[]){
    int thread;
    pthread_t *thread_handles;
    pthread_mutex_init(&mutex, NULL);

    thread_count = strtol(argv[1], NULL, 10);

    thread_handles = malloc(thread_count * sizeof(int));

    for (thread = 0; thread < thread_count; thread++){
        pthread_create(&thread_handles[thread], NULL, merge, (void*) thread);
    }

      for (thread = 0; thread < thread_count; thread++){
        pthread_join(thread_handles[thread], NULL);
    }

    pthread_mutex_destroy(&mutex);
    free(thread_handles);

}

