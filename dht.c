/**
 * dht.c
 *
 * CS 470 Project 4
 *
 * Implementation for distributed hash table (DHT).
 *
 * Names: Corwin Willms, Brooke Sindelar
 *
 */

#include <mpi.h>
#include <pthread.h>
#include "local.h"
#include<stdio.h>   

#include "dht.h"

#define PUT_REQUEST 1
#define GET_REQUEST 2
#define BREAK_LOOP 3
#define GET_SIZE 4

#define CLIENT_TO_SERVER 5
#define SERVER_TO_CLIENT 6

// mutex that guards interaction with local hash table
pthread_mutex_t hash_guard = PTHREAD_MUTEX_INITIALIZER;

// function prototypes
void* server_loop();
void dht_put(const char *key, long value);
long dht_get(const char *key);
size_t dht_size();
void dht_sync();
void dht_destroy(FILE *output);
int hash(const char *name);
void lock(pthread_mutex_t *mut);
void unlock(pthread_mutex_t *mut);

// struct for remote requests
typedef struct remote_request {
    int sender_rank;
    int req_type;
    long value;
    char key[MAX_KEYLEN];
} remote_req;

static int rank;
pthread_t* server;
int comm_sz;

/* 
Initialize MPI then use pthreads to spawn server threads.
Clients should return with their MPI rank. Server threads
should enter an infinite loop of receiving from any source.
*/
int dht_init()
{
    int provided;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
    if (provided != MPI_THREAD_MULTIPLE) {
        printf("ERROR: Cannot initialize MPI in THREAD_MULTIPLE mode.\n");
        exit(EXIT_FAILURE);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    local_init();

    server = malloc(sizeof(pthread_t));
    if (pthread_create(server, NULL, server_loop, NULL) != 0) {
            printf("ERROR: could not create thread\n");
            exit(EXIT_FAILURE);
        }

    // Barrier prevents clients from making requests before all
    // servers have been created.
    MPI_Barrier(MPI_COMM_WORLD);
    return rank;
}


/*
Server function. Server enters an infinite loop answering
requests from clients. 
*/
void* server_loop() {
    bool loop = true;
    long get_answer;
    struct remote_request *request = calloc(1, sizeof(struct remote_request));
    request->req_type = 5;

    // Enter infinite loop until all clients have finished
    while (loop) {
        MPI_Recv(request, // buffer 
                    sizeof(struct remote_request), // count
                    MPI_BYTE, // MPI_datatype 
                    MPI_ANY_SOURCE, // source
                    CLIENT_TO_SERVER, // tag
                    MPI_COMM_WORLD, // MPI_Comm
                    MPI_STATUS_IGNORE); // MPI_status 
        
        switch (request->req_type) {
            case PUT_REQUEST: 
                // Add the received key, value pair to local hash table
                lock(&hash_guard);
                local_put(request->key, request->value);
                unlock(&hash_guard);
                break;
            case GET_REQUEST:
                // Send requested value to client that requested it
                lock(&hash_guard);
                get_answer = local_get(request->key);
                unlock(&hash_guard);
                MPI_Send(&get_answer, // buffer 
                            1, // count
                            MPI_LONG, // MPI_datatype
                            request->sender_rank, // destination
                            SERVER_TO_CLIENT, // tag
                            MPI_COMM_WORLD); // MPI_Comm*/
                break;
            case BREAK_LOOP:
                // break out of server loop when all clients are done
                loop = false;
                break;
            case GET_SIZE:
                // send local size to the client that asked for it
                lock(&hash_guard);
                long my_size = local_size();
                unlock(&hash_guard);
                MPI_Send(&my_size, // buffer 
                            1, // count
                            MPI_LONG, // MPI_datatype
                            request->sender_rank, // destination
                            SERVER_TO_CLIENT, // tag
                            MPI_COMM_WORLD); // MPI_Comm*/
                break;

        }
    }
    free(request);
    return NULL;
}

/*
Adds key, value pair to remote hash table if necessary,
otherwise it just adds to the local hasj table. 
*/
void dht_put(const char *key, long value) 
{
    // check if key is 'owned' by another process
    int owner = hash(key);
    if (owner != rank)  {
        // remote procedure call to owner
        struct remote_request *req = calloc(1, sizeof(struct remote_request));
        req->sender_rank = rank;
        req->req_type = PUT_REQUEST;
        strncpy(req->key, key, sizeof(req->key));
        req->value = value;
        MPI_Send(req, // buffer (change to either MPI struct or find way to send C struct)
                    sizeof(struct remote_request), // count
                    MPI_BYTE, // MPI_datatype
                    owner, // destination
                    CLIENT_TO_SERVER, // tag
                    MPI_COMM_WORLD); // MPI_Comm*/
        free(req);
    } else {
        // add key, value pair to hash table
        lock(&hash_guard);
        local_put(key, value);
        unlock(&hash_guard);
    }

}

/*
Gets key, value pair from remote hash table if necessary,
otherwise it just gets from the local hasj table. 
*/
long dht_get(const char *key)
{
    long value = KEY_NOT_FOUND;

    // check if key is 'owned' by another process
    int owner = hash(key);
    if (owner != rank) { 
        // remote procedure call to owner
        struct remote_request *req = calloc(1, sizeof(struct remote_request));
        req->sender_rank = rank;
        req->req_type = GET_REQUEST;
        strncpy(req->key, key, sizeof(req->key));
        MPI_Send(req, // buffer 
                    sizeof(struct remote_request), // count
                    MPI_BYTE, // MPI_datatype
                    owner, // destination
                    CLIENT_TO_SERVER, // tag
                    MPI_COMM_WORLD); // MPI_Comm*/
        free(req);
        
        // Receive requested value
        MPI_Recv(&value, // buffer 
                    1, // count
                    MPI_LONG, // MPI_datatype 
                    owner, // source
                    SERVER_TO_CLIENT, // tag
                    MPI_COMM_WORLD, // MPI_Comm
                    MPI_STATUS_IGNORE); // MPI_status
    } else {
        lock(&hash_guard);
        value = local_get(key);
        unlock(&hash_guard);
    }
    return value;
}

/*
Finds the sum of every hash table. 
*/
size_t dht_size()
{ 
    // loop through and MPI_Ssend to all other procs
    // to let them know they need to send their size
    struct remote_request *req = calloc(1, sizeof(struct remote_request));
    for (int i = 0; i < comm_sz; i++) {
        if (i != rank) {
            req->sender_rank = rank;
            req->req_type = GET_SIZE;
            MPI_Send(req, // buffer 
                        sizeof(struct remote_request), // count
                        MPI_BYTE, // MPI_datatype
                        i, // destination
                        CLIENT_TO_SERVER, // tag
                        MPI_COMM_WORLD); // MPI_Comm
        }
    }
    free(req); 
    lock(&hash_guard);
    size_t global_size = local_size();
    unlock(&hash_guard);
    long size = 0;
    // Receive and add up all hash table sizes
    for (int i = 0; i < comm_sz; i++) {
        if (i != rank) {
            MPI_Recv(&size, // buffer 
                    1, // count
                    MPI_LONG, // MPI_datatype 
                    i, // source
                    SERVER_TO_CLIENT, // tag
                    MPI_COMM_WORLD, // MPI_Comm
                    MPI_STATUS_IGNORE); // MPI_status
        }
        global_size += size;
        size = 0; 
    }

    return global_size;
}

/*
Syncs all client threads with MPI_BArrier.
*/
void dht_sync()
{
    lock(&hash_guard);
    MPI_Barrier(MPI_COMM_WORLD);
    unlock(&hash_guard);
}

void dht_destroy(FILE *output)
{
    // Barrier to make sure all clients are done making requests before
    // servers are destroyed.
    MPI_Barrier(MPI_COMM_WORLD);    
    struct remote_request *req = calloc(1,sizeof(struct remote_request));
    req->sender_rank = rank;
    req->req_type = BREAK_LOOP;
    MPI_Send(req, // buffer 
                    sizeof(req), // count
                    MPI_BYTE, // MPI_datatype
                    rank, // destination
                    CLIENT_TO_SERVER, // tag
                    MPI_COMM_WORLD); // MPI_Comm

    free(req);
    // join server thread
    if (pthread_join(*server, NULL) != 0) {
            printf("ERROR: could not join pthread\n");
            exit(EXIT_FAILURE);
        }
    free(server);
    local_destroy(output);
    MPI_Finalize();
}

/*
 * given a key name, return the distributed hash table owner
 * (uses djb2 algorithm: http://www.cse.yorku.ca/~oz/hash.html)
 */
int hash(const char *name)
{
    unsigned hash = 5381;
    while (*name != '\0') {
        hash = ((hash << 5) + hash) + (unsigned)(*name++);
    }
    return hash % comm_sz;
}


void lock(pthread_mutex_t *mut)
{
    if (pthread_mutex_lock(mut) != 0) {
        printf("ERROR: could not acquire mutex\n");
        exit(EXIT_FAILURE);
    }
}

void unlock(pthread_mutex_t *mut)
{
    if (pthread_mutex_unlock(mut) != 0) {
        printf("ERROR: could not unlock mutex\n");
        exit(EXIT_FAILURE);
    }
}