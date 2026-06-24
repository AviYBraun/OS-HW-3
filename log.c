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
    // TODO: Return the full contents of the log as a dynamically allocated string
    // This function should handle concurrent access

    const char* dummy = "Log is not implemented.\n";
    int len = strlen(dummy);
    *dst = (char*)malloc(len + 1); // Allocate for caller
    if (*dst != NULL) {
        strcpy(*dst, dummy);
    }
    return len;
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len) {
    // TODO: Append the provided data to the log
    // This function should handle concurrent access
}
