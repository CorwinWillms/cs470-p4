/**
 * dht.c
 *
 * CS 470 Project 4
 *
 * Implementation for distributed hash table (DHT).
 *
 * Name: Corwin Willms, Brooke Sindelar
 *
 */

#include <mpi.h>
#include <pthread.h>

#include "dht.h"

/*
 * Private module variable: current process ID (MPI rank)
 */

// function prototypes
void* server_loop(void* rank);
void dht_put(const char *key, long value);
long dht_get(const char *key);
size_t dht_size();
void dht_sync();
void dht_destroy(FILE *output);


static int rank;
pthread_t* server;
int comm_sz;

// spawn and manage server thread along w/ dht_destroy()
/* 
The server thread should execute a loop that waits for remote procedure 
call requests from other processes and delegates them to the appropriate 
local methods
*/
int dht_init()
{
    // initialize MPI, use pthreads to spawn server threads.
    // clients should return with their MPI rank, server threads
    // should enter an infinite loop of receiving from any source
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    local_init();
    //spawn server threads after hashmap initialization
    server = malloc(sizeof(pthread_t));
    if (pthread_create(server, NULL, server_loop, (void*) rank) != 0) {
            printf("ERROR: could not create thread\n");
            exit(EXIT_FAILURE);
        }
    printf("Hello from main thread rank: %d\n", rank);
    return rank;
}

void* server_loop(void* rank) {
    //int x = 1;
    int my_rank = (int)rank;
    while (true) {
        printf("Hello from server thread rank: %d\n", my_rank);
        if (true){
            break;
        }
    }
    return NULL;
}

void dht_put(const char *key, long value)
{
    local_put(key, value);
}

long dht_get(const char *key)
{
    return local_get(key);
}

// requires MPI collective Comm
size_t dht_size()
{
    return local_size();
}

// requires MPI collective Comm
void dht_sync()
{
    // nothing to do in the serial version
}

void dht_destroy(FILE *output)
{
    local_destroy(output);
    if (pthread_join(*server, NULL) != 0) {
            printf("ERROR: could not join pthread\n");
            exit(EXIT_FAILURE);
        }
    MPI_Finalize();
}

