#include "segel.h"
#include "request.h"
#include "log.h"
#include <signal.h>
#include <sys/select.h>
#include <fcntl.h>


//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//

void* worker_thread_loop(void* arg); //forward declaration
float global_debug_sleep_time = 0.0; //make global so that logging function can access it


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

typedef struct PingNode {
    struct sockaddr_in addr;
    struct PingNode* next;
} PingNode;

typedef struct {
    PingNode* head;
    PingNode* tail;
    int count;
} PingQueue;


volatile int keep_running = 1;
TaskQueue shared_queue;
PingQueue* thread_pings; 
int udp_fd;
int notify_pipe[2];
server_log server_log_global;

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
    global_debug_sleep_time = *debug_sleep_time;
    
}




void init_queue(TaskQueue* q, int queue_size){
    q->tasks = malloc(sizeof(Task) * queue_size);
    q->capacity = queue_size;
    q->front = 0;
    q->rear = 0;
    q->count = 0;

    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_full, NULL);
    pthread_cond_init(&q->not_empty, NULL);
}




void handle_sigint(int sig){
    keep_running = 0;
}

int main(int argc, char *argv[])
{
    // Create the global server log
    signal(SIGINT, handle_sigint);
    server_log_global = create_log();

    int listenfd, connfd, tcp_portnum, udp_portnum, threads, queue_size, clientlen;
    float debug_sleep_time;
    struct sockaddr_in clientaddr;

    getargs(&tcp_portnum, &udp_portnum, &threads, &queue_size, &debug_sleep_time, argc, argv);

    init_queue(&shared_queue, queue_size);

    //CREATE WORKER THREAD POOL
    pthread_t *worker_threads = malloc(sizeof(pthread_t)*threads);
    threads_stats *thread_stats_array = malloc(sizeof(struct Threads_stats) * threads);

    for(int i = 0; i < threads; i++){
        thread_stats_array[i] = malloc(sizeof(struct Threads_stats));
        thread_stats_array[i]-> id = i + 1;
        thread_stats_array[i]-> stat_req = 0;
        thread_stats_array[i]-> dynm_req = 0;
        thread_stats_array[i]-> post_req = 0;
        thread_stats_array[i]-> total_req = 0;

        pthread_create(&worker_threads[i], NULL, worker_thread_loop, (void*)(thread_stats_array[i]));

    }
    thread_pings = malloc(sizeof(PingQueue)*threads);
    for(int i = 0; i < threads; i++){
        thread_pings[i].head = thread_pings[i].tail = NULL;
        thread_pings[i].count = 0;
    }
    //need to figure out how to balance listening on both tcp and udp sockets
    listenfd = Open_listenfd(tcp_portnum); //listen for incoming tasks via TCP protocol
    udp_fd = UDP_Open(udp_portnum);
    pipe(notify_pipe);
    fcntl(notify_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(notify_pipe[1], F_SETFL, O_NONBLOCK);

    while (keep_running) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        //set fd's for TCP and UDP
        FD_SET(listenfd, &read_fds);
        FD_SET(udp_fd, &read_fds);
        int max_fd = (listenfd > udp_fd) ? listenfd : udp_fd;

        int rc = select(max_fd + 1, &read_fds, NULL, NULL, NULL);

        if(rc < 0){
            if(errno == EINTR){
                continue;
            }
            unix_error("Select error"); 
        }
        //check UDP first
        if(FD_ISSET(udp_fd, &read_fds)){
            //implement logic for UDP stuff
            struct sockaddr_in client_udp_addr;
            char buffer[MAXBUF];

            int bytes_read = UDP_Read(udp_fd, &client_udp_addr, buffer, MAXBUF);
            if(bytes_read > 0){
                buffer[bytes_read] = '\0';

                int target_thread_id = atoi(buffer);

                int thread_idx = target_thread_id - 1;
                if(thread_idx >= 0 && thread_idx < threads){
                    PingNode* new_ping = malloc(sizeof(PingNode));
                    if(new_ping){
                        new_ping->addr = client_udp_addr;
                        new_ping->next = NULL;

                        pthread_mutex_lock(&shared_queue.lock);
                        if(thread_pings[thread_idx].tail){
                            thread_pings[thread_idx].tail->next = new_ping;
                        } else {
                            thread_pings[thread_idx].head = new_ping;
                        }
                        thread_pings[thread_idx].tail = new_ping;
                        thread_pings[thread_idx].count++;
                        pthread_cond_broadcast(&shared_queue.not_empty);
                        
                        pthread_mutex_unlock(&shared_queue.lock);
                    }
                }
            }
        }

        if(FD_ISSET(listenfd, &read_fds)){

            clientlen = sizeof(clientaddr);
            connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

            if(!keep_running){ 
            //in case ctr-C interrupts Accept(), in which case it will return -1, which if we use will cause a crash
                if(connfd >= 0){
                    Close(connfd);
                    break;
                }
            }
            //1) is taskQueue full? if yes - wait on worker_threads to free up tasks 

            //2) once taskQueue is notfull, create task, insert in taskqueue, send signals to worker_pool

            // gettimeofday(&arrival, NULL);
            pthread_mutex_lock(&shared_queue.lock);
            
            while(shared_queue.count == shared_queue.capacity){
                //wait on condition variable
                pthread_cond_wait(&shared_queue.not_full, &shared_queue.lock);
            }
                //when we wake up, we add the new task to the task list, and signal all of the threads who are waiting
            if(!keep_running){
                pthread_mutex_unlock(&shared_queue.lock);
                if(connffd >= 0){Close(connfd);}
                break;
            }
            Task new_task;
            new_task.connfd = connfd;
            gettimeofday(&new_task.arrival, NULL); //track the arrival time for task 3
                
            shared_queue.tasks[shared_queue.rear] = new_task;
            shared_queue.rear = (shared_queue.rear + 1) % shared_queue.capacity;
            shared_queue.count++;

            pthread_cond_signal(&shared_queue.not_empty);

            pthread_mutex_unlock(&shared_queue.lock);

        }

        
    }
    pthread_mutex_lock(&shared_queue.lock);
    pthread_cond_broadcast(&shared_queue.not_empty);
    pthread_mutex_unlock(&shared_queue.lock);

    for (int i =0; i<threads; i++){
        pthread_join(worker_threads[i],NULL);
    }
    // Clean up the server log before exiting
    destroy_log(server_log_global);

    for (int i = 0; i < threads; i++) {
        free(thread_stats_array[i]);
    }
    free(thread_stats_array);
    free(worker_threads);
    free(shared_queue.tasks);
    Close(udp_fd);
    close(notify_pipe[0]);
    close(notify_pipe[1]);
    free(thread_pings);
        
    pthread_mutex_destroy(&shared_queue.lock);
    pthread_cond_destroy(&shared_queue.not_full);
    pthread_cond_destroy(&shared_queue.not_empty);

    return 0;

}

void* worker_thread_loop(void* arg){
    //while loop of going to sleep, waiting for non-empty queue to call it, and performing tasks
    threads_stats my_stats = (threads_stats)(arg);
    char udp_send_buf[MAXBUF];
    int my_idx = my_stats->id - 1;
    while(1){
        pthread_mutex_lock(&shared_queue.lock);
        //first handle all UDP requests
        while(thread_pings[my_idx].head!=NULL){
            PingNode* ping_job = thread_pings[my_stats->id - 1].head;
            thread_pings[my_idx].head = ping_job->next;
            if(thread_pings[my_idx].head == NULL){
                thread_pings[my_idx].tail == NULL;
            }
            thread_pings[my_idx].count--;
        pthread_mutex_unlock(&shared_queue.lock);

        memset(udp_send_buf, 0, MAXBUF);
        sprintf(udp_send_buf, "Stat-Thread-Id:: %d\r\n", my_stats->id);
        sprintf(udp_send_buf+ strlen(udp_send_buf), "Stat-Thread-Count:: %d\r\n", my_stats->total_req);
        sprintf(udp_send_buf+ strlen(udp_send_buf), "Stat-Thread-Static:: %d\r\n", my_stats->stat_req);
        sprintf(udp_send_buf+ strlen(udp_send_buf), "Stat-Thread-Dynamic:: %d\r\n", my_stats->dynm_req);
        sprintf(udp_send_buf+ strlen(udp_send_buf), "Stat-Thread-Post:: %d\r\n", my_stats->post_req);

        UDP_Write(udp_fd, &ping_job->addr, udp_send_buf, strlen(udp_send_buf));
        free(ping_job);
        pthread_mutex_lock(&shared_queue.lock);

        }

        while(shared_queue.count == 0 && thread_pings[my_stats->id - 1].head == NULL && keep_running){
            pthread_cond_wait(&shared_queue.not_empty, &shared_queue.lock);
        }
        //check if a shutdown signal was received during sleep
        if (!keep_running && shared_queue.count == 0 && thread_pings[my_stats->id - 1].head == NULL){
            pthread_mutex_unlock(&shared_queue.lock);
            return NULL;
        }

        //check if a UDP ping woke up the thread - if so, restart the loop

        if(thread_pings[my_stats->id - 1].head != NULL){
            pthread_mutex_unlock(&shared_queue.lock);
            continue; 
        }

        Task job = shared_queue.tasks[shared_queue.front];
        shared_queue.front = (shared_queue.front + 1) % shared_queue.capacity;
        shared_queue.count --;

        pthread_cond_signal(&shared_queue.not_full); //since we took a task, we can wakeup master thread if it was waiting on a full queue
        pthread_mutex_unlock(&shared_queue.lock); //unlock before beginning to work on task, else we block out other threads

        time_stats tm_stats = {0};
        tm_stats.task_arrival = job.arrival;
        gettimeofday(&tm_stats.task_dispatch, NULL);


        requestHandle(job.connfd, tm_stats, my_stats, server_log_global);
        Close(job.connfd);

        
    }

    return NULL;
}
