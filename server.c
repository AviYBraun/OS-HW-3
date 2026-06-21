#include "segel.h"
#include "request.h"
#include "log.h"

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//

// Parses command-line arguments
void getargs(int *tcp_portnum, int *udp_portnum, int* threads, int* queue_size, float* debug_sleep_time, int argc, char *argv[])
{
    if (argc < 6) {
        fprintf(stderr, "Usage: %s <tcp_portnum> <udp_portnum> <threads> <queue_size> <debug_sleep_time>\n", argv[0]);
        exit(1);
    }
    *tcp_portnum = atoi(argv[1]);
    *udp_portnum = atoi(argv[2]);
    *threads = atoi(argv[3]);
    *queue_size= atoi(argv[4]);
    *debug_sleep_time = atof(argv[5]);
    
}

// TODO: HW3 — Task 1: Initialize the thread pool and request queue.
// This server currently handles all requests in the main thread.

// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).

// TODO: HW3 — Extend getargs() to parse the full argument list.

typedef struct{
    int connfd; //connector fd
    struct timeval arrival; //time when accepted by master thread
} Task;

typedef struct{
    //circular array for structs - size is set at queue_size
    Task* tasks; //the array
    int capacity;
    int front;
    int rear;
    int count;

    pthread_mutex_t lock;
    pthread_cond_t not_full; //signaled when a slot opens up
    pthread_cond_t not_empty; //signaled when a task is added

} TaskQueue;

void init_queue(TaskQueue* q, int queue_size){
    q->tasks = malloc(sizeof(Task) * queue_size);
    q->capacity = queue_size;
    q->front = 0;
    queue->rear = 0;
    queue->count = 0;

    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_full, NULL);
    pthread_cond_init(&q->not_empty, NULL);
}



//optional
struct active_queue{

}

int main(int argc, char *argv[])
{
    // Create the global server log
    server_log log = create_log();

    int listenfd, connfd, tcp_portnum, udp_portnum, threads, queue_size, clientlen;
    float debug_sleep_time;
    struct sockaddr_in clientaddr;

    getargs(&tcp_portnum, &udp_portnum, &threads, &queue_size, &debug_sleep_time argc, argv);

    TaskQueue shared_queue;
    init_queue(&shared_queue, queue_size);

    //CREATE WORKER THREAD POOL
    pthread_t *worker_threads = malloc(sizeof(pthread_t)*threads);
    threads_stats *thread_stats_array = malloc(sizeof(struct Threads_stats) * threads);

    for(int i = 0; i < threads; i++){
        threads_stats_array[i] = malloc(sizeof(struct Threads_stats));
        threads_stats_array[i]-> id = i + 1;
        threads_stats_array[i]-> stat_req = 0;
        threads_stats_array[i]-> dynm_req = 0;
        threads_stats_array[i]-> post_req = 0;
        threads_stats_array[i]-> total_req = 0;

        pthread_create(&threads[i], NULL, worker_thread_loop, threads_stats);

    }


    listenfd = Open_listenfd(port);
    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

        // TODO: HW3 — Record the request arrival time here.

        // DEMO PURPOSE ONLY:
        // This is a dummy request handler that immediately processes the
        // request in the master thread without concurrency. Replace this with
        // logic that enqueues the connection so a worker thread handles it.

        threads_stats t = malloc(sizeof(struct Threads_stats));
        t->id = 0;             // Thread ID (placeholder)
        t->stat_req = 0;       // Static request count
        t->dynm_req = 0;       // Dynamic request count
        t->post_req = 0;       // POST request count
        t->total_req = 0;      // Total request count

        time_stats dum;

        // gettimeofday(&arrival, NULL);

        // Call the request handler (immediate in master thread — DEMO ONLY)
        requestHandle(connfd, dum, t, log);

        free(t); // Cleanup
        Close(connfd); // Close the connection
    }

    // Clean up the server log before exiting
    destroy_log(log);

    // TODO: HW3 — Add cleanup code for the thread pool and queue.
}

void worker_thread_loop(){
    //while loop of going to sleep, waiting for non-empty queue to call it, and performing tasks


    return;
}
