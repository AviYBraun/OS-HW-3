#include "segel.h"
#include "request.h"
#include "log.h"
#include <signal.h>


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



global int keep_running = 1;

void handle_sigint(int sig){
    keep_running = 0;
}

int main(int argc, char *argv[])
{
    // Create the global server log
    signal(SIGINT, handle_sigint);
    server_log log = create_log();

    int listenfd, connfd, tcp_portnum, udp_portnum, threads, queue_size, clientlen;
    float debug_sleep_time;
    struct sockaddr_in clientaddr;

    getargs(&tcp_portnum, &udp_portnum, &threads, &queue_size, &debug_sleep_time, argc, argv);

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

        pthread_create(&worker_threads[i], NULL, worker_thread_loop, void*(threads_stats_array[i]));

    }

    //need to figure out how to balance listening on both tcp and udp sockets
    listenfd_tcp = Open_listenfd(&tcp_portnum); //listen for incoming tasks via TCP protocol
    listenfd_udp = Open_listenfd(&udp_portnum);

    while (keep_running) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd_tcp, (SA *)&clientaddr, (socklen_t*) &clientlen);

        //1) is taskQueue full? if yes - wait on worker_threads to free up tasks 

        //2) once taskQueue is notfull, create task, insert in taskqueue, send signals to worker_pool

        // gettimeofday(&arrival, NULL);
        pthread_mutex_lock(shared_queue.lock);
        
        while(shared_queue.count == shared_queue.capacity){
            //wait on condition variable
            pthread_cond_wait(&shared_queue.not_full, &shared_queue.lock);
        }
            //when we wake up, we add the new task to the task list, and signal all of the threads who are waiting
            Task new_task;
            new_task.connfd = connfd;
            gettimeofday(&new_task.arrival, NULL); //track the arrival time for task 3
            
            shared_queue.tasks[shared_queue.read] = new_task;
            shared_queue.rear = shared_queue.rear + 1 % shared_queue.capacity;
            shared_queue.count++;

            pthread_cond_signal(&shared_queue.not_empty);


        p_thread_mutex_unlock(&shared_queue.lock);

        //requestHandle(connfd, dum, t, log); - this is moved to the worker thread function for them to work on the tasks

        
        // Close(connfd); // Close the connection - moved to the worker thread function
    }

    // Clean up the server log before exiting
    destroy_log(log);

    for (int i = 0; i < threads; i++) {
        free(thread_stats_array[i]);
    }
    free(thread_stats_array);
    free(worker_threads);
    free(shared_queue.tasks);
    
    pthread_mutex_destroy(&shared_queue.lock);
    pthread_cond_destroy(&shared_queue.not_full);
    pthread_cond_destroy(&shared_queue.not_empty);

    return 0;

}

void* worker_thread_loop(void* arg){
    //while loop of going to sleep, waiting for non-empty queue to call it, and performing tasks
    threads_stats my_stats = (threads_stats)(arg);
    while(1){
        pthread_mutex_lock(&shared_queue.lock);

        while(shared_queue.count == 0){
            pthread_cond_wait(&shared_queue.not_empty, &shared_queue.lock);
        }

        Task job = shared_queue.tasks[shared_queue.front];
        shared_queue.front = (shared_queue.front + 1) % shared_queue.capacity;
        shared_queue.count --;

        pthread_cond_signal(&shared_queue.not_full); //since we took a task, we can wakeup master thread if it was waiting on a full queue
        pthread_mutex_unlock(&shared_queue.lock); //unlock before beginning to work on task, else we block out other threads

        time_stats tm_stats;
        tm_stats.task_arrival = job.arrival;
        gettimeofday(&tm_stats.task_dispatch, NULL);


        requestHandle(job.connfd, tm_stats, my_stats, log);
        shared_queue.front = shared_queue.front + 1 % shared_queue.capacity;

        
    }

    return;
}
