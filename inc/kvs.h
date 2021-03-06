#pragma once

#include <stdio.h> // FILE*

#define KVS_INIT {NULL, 0, NULL, 0, NULL}

#define KVS_OK                 (0)
#define KVS_ERROR              (-1)
#define KVS_KEY_ALREADY_EXISTS (-2)
#define KVS_KEY_DOES_NOT_EXIST (-2)

#define KVS_DEFRAG_PATH (".defrag")

typedef struct kvs_value_t
{
    size_t size;
    char*  data;
} kvs_value_t;

typedef struct kv_t
{
    char   used;
    char   ksize;
    //alignment
    short  unused;
    int    vsize;
} kv_t;

typedef struct kv_index_t
{
  long offset;
  struct kv_index_t *prev;
  struct kv_index_t *next;
} kv_index_t;

typedef struct kvs_t
{
    char* path;
    int fd;
    void* base;
    size_t map_size;
    kv_index_t* kv_index;
} kvs_t;

int kvs_open(kvs_t* kvs, char* path);

void kvs_close(kvs_t* kvs);

int kvs_has(kvs_t* kvs, char* key);

int kvs_get(kvs_t* kvs, char* key, kvs_value_t* value);

int kvs_set(kvs_t* kvs, char* key, kvs_value_t* value);

int kvs_delete(kvs_t* kvs, char* key);

int kvs_defragment(kvs_t* kvs);
