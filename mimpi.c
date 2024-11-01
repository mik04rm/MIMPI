/**
 * This file is for implementation of MIMPI library.
 * */

#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#define MAX(a, b) (((a) > (b)) ? (a) : (b))

#define MAX_CHUNK_SIZE 512
#define ANY_TAG 0
#define BARRIER_TAG -1
#define BCAST_TAG -2
#define REDUCE_TAG -3

static int world_size;
static int rank;
static int read_dsc[MAX_WORLD_SIZE];
static int write_dsc[MAX_WORLD_SIZE];

static pthread_t threads[MAX_WORLD_SIZE];

typedef struct Message {
    int count, tag;
    char *content;
    struct Message *list_next;
} Message;

typedef struct First_chunk {
    int32_t count, tag;
    char content[MAX_CHUNK_SIZE - 2 * sizeof(int32_t)];
} First_chunk;

static pthread_mutex_t mutex;
static pthread_cond_t for_matching_message;
static int wanted_source = -1;
static int wanted_count;
static int wanted_tag;
static Message *mess_list_head[MAX_WORLD_SIZE];
static Message *mess_list_tail[MAX_WORLD_SIZE];

static bool receiver_finished[MAX_WORLD_SIZE];

static bool is_matching(Message *mess, int count, int tag) {
    return mess->count == count && (mess->tag == tag || tag == ANY_TAG);
}

static void *receiver(void *arg) {
    int source = *((int *)arg);
    free(arg);

    First_chunk *first_chunk = malloc(sizeof(First_chunk));

    bool should_break = false;

    while (true) {

        int chrecv_ret =
            chrecv(read_dsc[source], first_chunk, sizeof(*first_chunk));

        if ((chrecv_ret == -1 && errno == EBADF) || chrecv_ret == 0) {
            break;
        }
        ASSERT_SYS_OK(chrecv_ret);

        int64_t chunk_start =
            MIN(first_chunk->count, sizeof(First_chunk) - 2 * sizeof(int32_t));

        char *content = NULL;
        if (first_chunk->count > 0) {
            content = malloc(sizeof(char) * first_chunk->count);
            memcpy(content, first_chunk->content, chunk_start);
        }

        while (chunk_start < first_chunk->count) {
            int chunk_size =
                MIN(first_chunk->count - chunk_start, MAX_CHUNK_SIZE);

            chrecv_ret =
                chrecv(read_dsc[source], content + chunk_start, chunk_size);
            if ((chrecv_ret == -1 && errno == EBADF) || chrecv_ret == 0) {
                should_break = true;
                free(content);
                break;
            }
            ASSERT_SYS_OK(chrecv_ret);

            chunk_start += MAX_CHUNK_SIZE;
        }

        if (should_break) {
            break;
        }

        // all chunks received

        Message *message = malloc(sizeof(Message));
        message->count = first_chunk->count;
        message->tag = first_chunk->tag;
        message->content = content;
        message->list_next = NULL;

        ASSERT_ZERO(pthread_mutex_lock(&mutex));

        // adding to the end of the list
        if (mess_list_tail[source] != NULL) {
            mess_list_tail[source]->list_next = message;
        } else {
            mess_list_head[source] = message;
        }
        mess_list_tail[source] = message;

        if (wanted_source == source &&
            is_matching(message, wanted_count, wanted_tag)) {
            wanted_source = -1;
            ASSERT_ZERO(pthread_cond_signal(&for_matching_message));
        }

        ASSERT_ZERO(pthread_mutex_unlock(&mutex));
    }
    free(first_chunk);

    ASSERT_ZERO(pthread_mutex_lock(&mutex));

    receiver_finished[source] = true;
    if (wanted_source == source) {
        wanted_source = -1;
        ASSERT_ZERO(pthread_cond_signal(&for_matching_message));
    }

    ASSERT_ZERO(pthread_mutex_unlock(&mutex));

    int *ret = malloc(sizeof(int));
    *ret = 0;

    return ret;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    world_size = atoi(getenv("MIMPI_WORLD_SIZE"));
    rank = atoi(getenv("MIMPI_RANK"));

    for (int i = 0; i < world_size; i++) {
        if (i == rank) {
            continue;
        }
        char dsc_str[ENVNAME_BUF_SIZE];
        snprintf(dsc_str, sizeof(dsc_str), "MIMPI_READ_DSC_%d", i);
        read_dsc[i] = atoi(getenv(dsc_str));

        snprintf(dsc_str, sizeof(dsc_str), "MIMPI_WRITE_DSC_%d", i);
        write_dsc[i] = atoi(getenv(dsc_str));
    }

    ASSERT_ZERO(pthread_mutex_init(&mutex, NULL));

    for (int i = 0; i < world_size; i++) {
        if (i == rank) {
            continue;
        }
        ASSERT_ZERO(pthread_cond_init(&for_matching_message, NULL));
    }

    for (int i = 0; i < world_size; i++) {
        if (i == rank) {
            continue;
        }
        int *receiver_arg = malloc(sizeof(int));
        *receiver_arg = i;
        ASSERT_ZERO(pthread_create(&threads[i], NULL, receiver, receiver_arg));
    }
}

void MIMPI_Finalize() {

    for (int i = 0; i < world_size; i++) {
        if (i == rank) {
            continue;
        }
        ASSERT_SYS_OK(close(read_dsc[i]));
        ASSERT_SYS_OK(close(write_dsc[i]));
    }

    for (int i = 0; i < world_size; i++) {
        if (i == rank) {
            continue;
        }
        ASSERT_ZERO(pthread_join(threads[i], NULL));
    }
    channels_finalize();

    for (int i = 0; i < world_size; i++) {
        if (i == rank) {
            continue;
        }
        Message *mess = mess_list_head[i];
        while (mess != NULL) {
            Message *next_mess = mess->list_next;
            free(mess);
            mess = next_mess;
        }
    }
    ASSERT_ZERO(pthread_cond_destroy(&for_matching_message));
    ASSERT_ZERO(pthread_mutex_destroy(&mutex));
}

int MIMPI_World_size() { return world_size; }

int MIMPI_World_rank() { return rank; }

MIMPI_Retcode MIMPI_Send(void const *data, int count, int destination,
                         int tag) {

    if (destination == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }

    if (destination < 0 || destination >= world_size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    First_chunk *first_chunk = malloc(sizeof(First_chunk));
    first_chunk->count = count;
    first_chunk->tag = tag;

    int64_t chunk_start = MIN(count, sizeof(first_chunk->content));

    memset(first_chunk->content, 0, sizeof(first_chunk->content));

    if (count > 0) {
        memcpy(first_chunk->content, data, chunk_start);
    }

    int chsend_ret =
        chsend(write_dsc[destination], first_chunk, sizeof(*first_chunk));
    free(first_chunk);
    if (chsend_ret == -1 && errno == EPIPE) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }
    ASSERT_SYS_OK(chsend_ret);

    while (chunk_start < count) {

        int chunk_size = MIN(MAX_CHUNK_SIZE, count - chunk_start);

        int chsend_ret =
            chsend(write_dsc[destination], data + chunk_start, chunk_size);
        if (chsend_ret == -1 && errno == EPIPE) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        ASSERT_SYS_OK(chsend_ret);

        chunk_start += MAX_CHUNK_SIZE;
    }
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(void *data, int count, int source, int tag) {

    if (source == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }

    if (source < 0 || source >= world_size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    ASSERT_ZERO(pthread_mutex_lock(&mutex));

    Message *matching_mess = mess_list_head[source];
    Message *prev_mess = NULL;
    while (matching_mess != NULL && !is_matching(matching_mess, count, tag)) {
        prev_mess = matching_mess;
        matching_mess = matching_mess->list_next;
    }

    if (matching_mess == NULL) {
        wanted_source = source;
    } else {
        wanted_source = -1;
    }

    if (wanted_source != -1 && receiver_finished[source]) {
        ASSERT_ZERO(pthread_mutex_unlock(&mutex));
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    wanted_count = count;
    wanted_tag = tag;
    while (wanted_source != -1) {
        pthread_cond_wait(&for_matching_message, &mutex);
    }

    matching_mess = mess_list_head[source];
    prev_mess = NULL;
    while (matching_mess != NULL && !is_matching(matching_mess, count, tag)) {
        prev_mess = matching_mess;
        matching_mess = matching_mess->list_next;
    }

    if (matching_mess == NULL) {
        ASSERT_ZERO(pthread_mutex_unlock(&mutex));
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    Message *next_mess = matching_mess->list_next;

    if (prev_mess == NULL) {
        mess_list_head[source] = next_mess;
    } else {
        prev_mess->list_next = next_mess;
    }

    if (next_mess == NULL) {
        mess_list_tail[source] = prev_mess;
    }

    if (count > 0) {
        memcpy(data, matching_mess->content, count);
    }
    free(matching_mess->content);
    free(matching_mess);

    ASSERT_ZERO(pthread_mutex_unlock(&mutex));

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    MIMPI_Retcode ret;

    ret = MIMPI_Recv(NULL, 0, 2 * rank + 1, BARRIER_TAG);
    if (ret == MIMPI_ERROR_REMOTE_FINISHED) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    ret = MIMPI_Recv(NULL, 0, 2 * rank + 2, BARRIER_TAG);
    if (ret == MIMPI_ERROR_REMOTE_FINISHED) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    MIMPI_Send(NULL, 0, (rank - 1) / 2, BARRIER_TAG);

    ret = MIMPI_Recv(NULL, 0, (rank - 1) / 2, BARRIER_TAG);
    if (ret == MIMPI_ERROR_REMOTE_FINISHED) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    MIMPI_Send(NULL, 0, 2 * rank + 1, BARRIER_TAG);
    MIMPI_Send(NULL, 0, 2 * rank + 2, BARRIER_TAG);

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(void *data, int count, int root) {

    int tree_idx = (rank - root + world_size) % world_size;

    bool has_parent = (tree_idx != 0);
    bool has_left_child = (2 * tree_idx + 1 < world_size);
    bool has_right_child = (2 * tree_idx + 2 < world_size);

    int parent = ((tree_idx - 1) / 2 + root) % world_size;
    int left_child = (2 * tree_idx + 1 + root) % world_size;
    int right_child = (2 * tree_idx + 2 + root) % world_size;

    MIMPI_Retcode ret;

    if (has_left_child) {
        ret = MIMPI_Recv(NULL, 0, left_child, BCAST_TAG);
        if (ret == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (has_right_child) {
        ret = MIMPI_Recv(NULL, 0, right_child, BCAST_TAG);
        if (ret == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (has_parent) {
        MIMPI_Send(NULL, 0, parent, BCAST_TAG);
        ret = MIMPI_Recv(data, count, parent, BCAST_TAG);
        if (ret == MIMPI_ERROR_REMOTE_FINISHED) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (has_left_child) {
        MIMPI_Send(data, count, left_child, BCAST_TAG);
    }

    if (has_right_child) {
        MIMPI_Send(data, count, right_child, BCAST_TAG);
    }

    return MIMPI_SUCCESS;
}

static void reduce(int8_t *dest, int8_t *src, int count, MIMPI_Op op) {
    for (int i = 0; i < count; i++) {
        if (op == MIMPI_MAX) {
            dest[i] = MAX(dest[i], src[i]);
        } else if (op == MIMPI_MIN) {
            dest[i] = MIN(dest[i], src[i]);
        } else if (op == MIMPI_SUM) {
            dest[i] += src[i];
        } else if (op == MIMPI_PROD) {
            dest[i] *= src[i];
        }
    }
}

MIMPI_Retcode MIMPI_Reduce(void const *send_data, void *recv_data, int count,
                           MIMPI_Op op, int root) {
    int tree_idx = (rank - root + world_size) % world_size;

    bool has_parent = (tree_idx != 0);
    bool has_left_child = (2 * tree_idx + 1 < world_size);
    bool has_right_child = (2 * tree_idx + 2 < world_size);

    int parent = ((tree_idx - 1) / 2 + root) % world_size;
    int left_child = (2 * tree_idx + 1 + root) % world_size;
    int right_child = (2 * tree_idx + 2 + root) % world_size;

    MIMPI_Retcode ret;
    int8_t *data = malloc(count);
    int8_t *child_data = malloc(count);
    if (count > 0) {
        memcpy(data, send_data, count);
    }

    if (has_left_child) {
        ret = MIMPI_Recv(child_data, count, left_child, REDUCE_TAG);
        reduce(data, child_data, count, op);
        if (ret == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            free(child_data);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (has_right_child) {
        ret = MIMPI_Recv(child_data, count, right_child, REDUCE_TAG);
        reduce(data, child_data, count, op);
        if (ret == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            free(child_data);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (has_parent) {
        MIMPI_Send(data, count, parent, REDUCE_TAG);

        ret = MIMPI_Recv(NULL, 0, parent, REDUCE_TAG);
        if (ret == MIMPI_ERROR_REMOTE_FINISHED) {
            free(data);
            free(child_data);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

    } else {
        if (count > 0) {
            memcpy(recv_data, data, count);
        }
    }

    if (has_left_child) {
        MIMPI_Send(NULL, 0, left_child, REDUCE_TAG);
    }

    if (has_right_child) {
        MIMPI_Send(NULL, 0, right_child, REDUCE_TAG);
    }

    free(data);
    free(child_data);

    return MIMPI_SUCCESS;
}