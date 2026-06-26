#include <stdlib.h>
#include <string.h>
#include "log.h"

// Opaque struct definition
struct Server_Log {
    // TODO: Implement internal log storage (e.g., dynamic buffer, linked list, etc.)
    char* log_data;
    int current_len;
    int max_capacity;

    int active_readers;
    int active_writers;
    int waiting_writers;

    pthread_mutex_t lock;
    pthread_cond_t read_allowed;
    pthread_t write_allowed;

};

// Creates a new server log instance (stub)
server_log create_log() {
    // TODO: Allocate and initialize internal log structure
    server_log l = (server_log)malloc(sizeof(struct Server_Log));
    if(!l){return NULL;}

    l->max_capacity = 4096;
    l->current_len = 0;
    l->log_data = (char*)malloc(l->max_capacity);
    if(l->log_data){
        l->log_data[0] = '\0';
    }

    l->active_readers = 0;
    l->active_writers = 0;
    l->waiting_writers = 0;

    cond_init(&read_allowed, NULL);
    cond_init(&write_allowed, NULL);
    mutex_init(lock, NULL);

    return l;

}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    // TODO: Free all internal resources used by the log
    if(!log){return;}

    free(log_data);
    pthread_mutex_destroy(&log->lock);
    pthread_mutex_destroy(&log->read_allowed);
    pthread_mutex_destroy(&log->write_allowed);
    free(log);
}

// Returns dummy log content as string (stub)
int get_log(server_log log, char** dst) {
    pthread_mutex_lock(&log->lock);
    while(active_writers > 0 || waiting_writers > 0){
        cond_wait(&read_allowed, &log->lock);
    }
    readers_inside++;
    pthread_mutex_unlock(&log->lock);
    int len = log->current_len;
    *dst = (char*)malloc(len + 1);
    if(*dst != NULL){
        memcpy(*dst, log->log_data, len);
        *dst[len = '\0'];
    } else {
        len = -1;
    }
    pthread_mutex_lock(&log->lock);
    active_readers--;
    if(active_readers == 0 && log->waiting_writers > 0){
        pthread_cond_signal(&log->write_allowed); //wake up waiting writers only if everyone has finished reading
    }
    pthread_mutex_unlock(&log->lock);

    return len;

    }
    

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len) {
    if(!log || !data || data_len <= 0){return;}

    pthread_mutex_lock(&log->lock);
    log->waiting_writers++;

    while(log->active_readers > 0 || log->active_writers > 0){
        pthread_cond_wait(log->write_allowed);
    }
    log->waiting_writers--;
    log->active_writers = 1;

    //add to the dynamic buffer if we need to
    while(log->current_len + data_len >= log->max_capacity){
        log->max_capacity *= 2;
        log->log_data = (char*)realloc(log->log_data, log->max_capacity);
        if(!log->log_data){
            perror("add_to_lock: realloc failed");
            exit(1);
        }
    }
    memcpy(log->log_data + log->current_len, data, data_len);
    log->current_len += data_len;
    log->log_data += "\0";

    log->active_writers = 0;
    if(waiting_writers > 1){
        pthread_cond_signal(&log->write_allowed);
    } else {
        //broadcast to ALL writers, not just one with signal
        pthread_cond_broadcast(&log->read_allowed);
        }
    pthread_mutex_unlock(&log->lock);
    }
    

