#pragma once

#include <stdio.h> // FILE*

#define KVS_INIT {NULL, NULL, NULL}

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
    long   addr;
    char   used;
    char   ksize;
    int    vsize;
    char*  key;
    struct kv_t* prev;
    struct kv_t* next;
} kv_t;

typedef struct kvs_t
{
    char* path;
    FILE* fp;
    kv_t* kv;
} kvs_t;

int kvs_open(kvs_t* kvs, char* path);

void kvs_close(kvs_t* kvs);

int kvs_has(kvs_t* kvs, char* key);

int kvs_get(kvs_t* kvs, char* key, kvs_value_t* value);

int kvs_set(kvs_t* kvs, char* key, kvs_value_t* value);

int kvs_delete(kvs_t* kvs, char* key);

int kvs_defragment(kvs_t* kvs);
