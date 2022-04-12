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
int hash(const char *name);

typedef struct remote_request {
    int sender_rank;
    char req_type[MAX_LINE_LEN];
    char key[MAX_LINE_LEN];
} remote_req;

static int rank;
pthread_t* server;
int comm_sz;

/**
HINT: Work incrementally! Don't try to implement the entire protocol at once. 
Start by augmenting the init and destroy routines to spawn and clean up the 
server thread (and make sure init returns the process's MPI rank). Then 
implement the remote put functionality (asynchronously at first), using 
the end-of-execution dump files to check for correctness. You will also 
need to make sure that the server threads do not exit until all clients 
have finished processing commands. Once that is working, implement sync 
and size (which will require a request/response RPC). This will help you 
change the put functionality to be synchronous (as required by the above 
spec). Finally, you can use all of that knowledge to implement the get function. 
*/

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
    char *key = NULL;
    while (true) {
        printf("Hello from server thread rank: %d\n", my_rank);
        /* commented until sending structs is figured out
        struct remote_request *request;
        MPI_recv(&request, // buffer 
                    sizeof(remote_request), // count
                    MPI_BYTE, // MPI_datatype (possibly created mpi struct type or some type for C struct)
                    MPI_ANY_SOURCE, // source
                    0, // tag
                    MPI_COMM_WORLD, // MPI_Comm
                    MPI_STATUS_IGNORE); // MPI_status */
        if (true){
            break;
        }
    }
    return NULL;
}

void dht_put(const char *key, long value)
{
    // check if key is 'owned' by another process
    int owner = hash(key);
    if (false)  {// commented until finished with simpler functions(owner != rank) {
        // remote procedure call to owner
        struct remote_request req;
        req.sender_rank = rank;
        req.req_type = "put";
        req.key = key;
        MPI_Ssend(&req, // buffer (change to either MPI struct or find way to send C struct)
                    strlen(key), // count
                    MPI_BYTE, // MPI_datatype
                    owner, // destination
                    0, // tag
                    MPI_COMM_WORLD); // MPI_Comm
    } else {
        local_put(key, value);
    }

}

long dht_get(const char *key)
{
    long value = KEY_NOT_FOUND;
    // check if key is 'owned' by another process
    int owner = hash(key);
    if (false) { // commented until finished with simpler functions(owner != rank) {
        // remote procedure call to owner
        MPI_Ssend(key, // buffer (change to either MPI struct or find way to send C struct)
                    strlen(key), // count
                    MPI_CHAR, // MPI_datatype
                    owner, // destination
                    0, // tag
                    MPI_COMM_WORLD); // MPI_Comm
        
        MPI_Recv(&value, // buffer 
                    1, // count
                    MPI_LONG, // MPI_datatype (need to receive a size_t, possibly create struct type)
                    owner, // source
                    0, // tag
                    MPI_COMM_WORLD, // MPI_Comm
                    MPI_STATUS_IGNORE); // MPI_status
    } else {
        value = local_get(key);
    }

    return value;
}

// requires MPI collective Comm
size_t dht_size()
{ /*
    // loop through and MPI_Ssend to all other procs
    // to let them know to perform the reduction
    for (int i = 0; i < comm_sz; i++) {
        if (i != rank) {
            MPI_Ssend(key, // buffer (change to either MPI struct or find way to send C struct)
                    strlen(key), // count
                    MPI_CHAR, // MPI_datatype
                    owner, // destination
                    0, // tag
                    MPI_COMM_WORLD); // MPI_Comm
        }
    }

    size_t size = local_size();
    size_t global_size = 0;
    MPI_Reduce(&size, // send buffer
                    &global_size, // receiving buffer 
                    comm_sz, // count
                    MPI_LONG, // MPI_datatype
                    MPI_SUM, //MPI_Op
                    rank, // 'root'
                    MPI_COMM_WORLD); // MPI_Comm 
    return global_size //local_size();
    */
    return local_size();
}

// requires MPI collective Comm
void dht_sync()
{
    // nothing to do in the serial version
    //MPI_Barrier(MPI_COMM_WORLD);
}

void dht_destroy(FILE *output)
{
    // Barrier untill all clients have finished processing commands
    // then MPI_Ssend to all clients telling them to exit infinite loop
    local_destroy(output);

    // join server thread
    if (pthread_join(*server, NULL) != 0) {
            printf("ERROR: could not join pthread\n");
            exit(EXIT_FAILURE);
        }
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