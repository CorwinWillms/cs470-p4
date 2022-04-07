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
static int rank;

// spawn and manage server thread along w/ dht_destroy()
/* 
The server thread should execute a loop that waits for remote procedure 
call requests from other processes and delegates them to the appropriate 
local methods
*/
int dht_init()
{
    local_init();
    // initialize MPI, use pthreads to spawn server threads.
    // clients should return with their MPI rank, server threads
    // should enter an infinite loop of receiving from any source
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    
    return rank;
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
    MPI_Finalize()
}

