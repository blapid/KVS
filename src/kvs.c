#include "kvs.h"

#include <stdio.h>  // fopen, fclose, fseek, ftell, feof, rename, SEEK_SET, SEEK_END, SEEK_CUR
#include <stdlib.h> // malloc, memset
#include <string.h> // strlen, strncmp
#include <unistd.h> // access, F_OK

#define KVS_HEADER_SIZE (sizeof(char) + sizeof(char) + sizeof(int))
#define KVS_ALIGN(n)    (((n) % KVS_HEADER_SIZE == 0) ? (n) : (n) + (KVS_HEADER_SIZE - ((n) % KVS_HEADER_SIZE)))
#define KVS_PADDING(n)  (KVS_ALIGN(n) - (n))
#define KVS_SIZE(kv)    (KVS_ALIGN((kv)->ksize + (kv)->vsize))
#define FOR(kvs, i)     for (i = kvs->kv->next; i != NULL; i = i->next)

static char* kvs_read_value(kvs_t* kvs, kv_t* kv)
{
    char* data = NULL;

    if (fseek(kvs->fp, kv->addr + KVS_HEADER_SIZE + kv->ksize, SEEK_SET) != 0)
    {
        return NULL;
    }

    data = (char*) malloc(kv->vsize);
    if (data == NULL)
    {
        return NULL;
    }

    if (fread((void*) data, 1, kv->vsize, kvs->fp) != kv->vsize)
    {
        free(data);
        return NULL;
    }

    return data;
}

static kv_t* kvs_allocate_kv(long addr, char used, char ksize, int vsize, char* key)
{
    kv_t* kv = NULL;

    kv = (kv_t*) malloc(sizeof(kv_t));
    if (kv == NULL)
    {
        return NULL;
    }

    kv->addr  = addr;
    kv->used  = used;
    kv->ksize = ksize;
    kv->vsize = vsize;
    kv->key   = key;
    kv->prev  = NULL;
    kv->next  = NULL;

    return kv;
}

static void kvs_deallocate_kv(kv_t* kv)
{
    if (kv->key != NULL)
    {
        free(kv->key);
    }

    free(kv);
}

static void kvs_append_kv(kvs_t* kvs, kv_t* kv, kv_t* new_kv)
{
    if (kv->next != NULL)
    {
        kv->next->prev = new_kv;
    }
    new_kv->next = kv->next;
    kv->next = new_kv;
    new_kv->prev = kv;

    if (kvs->kv->prev == kv)
    {
        kvs->kv->prev = new_kv;
    }
}

static void kvs_insert_kv(kvs_t* kvs, kv_t* kv)
{
    if (kvs->kv->prev == NULL)
    {
        kvs->kv->next = kv;
        kv->prev = kvs->kv;
        kvs->kv->prev = kv;
    }
    else
    {
        kvs_append_kv(kvs, kvs->kv->prev, kv);
    }
}

static void kvs_delete_kv(kvs_t* kvs, kv_t* kv)
{
    if (kv->next != NULL)
    {
        kv->next->prev = kv->prev;
    }
    kv->prev->next = kv->next;

    if (kvs->kv->prev == kv)
    {
        kvs->kv->prev = kv->prev;
    }
}

static int kvs_write_header(FILE* fp, kv_t* kv)
{
    if (fseek(fp, kv->addr, SEEK_SET) != 0)
    {
        return -1;
    }

    if (fwrite((void*) &(kv->used), sizeof(kv->used), 1, fp) != 1)
    {
        return -1;
    }

    if (fwrite((void*) &(kv->ksize), sizeof(kv->ksize), 1, fp) != 1)
    {
        return -1;
    }

    if (fwrite((void*) &(kv->vsize), sizeof(kv->vsize), 1, fp) != 1)
    {
        return -1;
    }

    return 0;
}

static int kvs_write_data(FILE* fp, kv_t* kv, char* data)
{
    char padding_size = 0;
    char padding[KVS_HEADER_SIZE] = {0};

    if (kvs_write_header(fp, kv) != 0)
    {
        return -1;
    }

    if (fwrite((void*) kv->key, sizeof(char), kv->ksize, fp) != kv->ksize)
    {
        return -1;
    }

    if (fwrite((void*) data, sizeof(char), kv->vsize, fp) != kv->vsize)
    {
        return -1;
    }

    padding_size = KVS_PADDING(kv->ksize + kv->vsize);

    if (padding_size)
    {
        if (fwrite((void*) padding, sizeof(char), padding_size, fp) != padding_size)
        {
            return -1;
        }
    }

    return 0;
}

static int kvs_load(kvs_t* kvs)
{
    long  addr  = 0;
    char  used  = 0;
    char  ksize = 0;
    int   vsize = 0;
    char* key   = NULL;
    kv_t* kv    = NULL;

    while (1)
    {
        addr = ftell(kvs->fp);
        if (addr == -1)
        {
            return -1;
        }

        if (fread((void*) &used, sizeof(used), 1, kvs->fp) != 1)
        {
            if (feof(kvs->fp))
            {
                break;
            }
            else
            {
                return -1;
            }
        }

        if (fread((void*) &ksize, sizeof(ksize), 1, kvs->fp) != 1)
        {
            return -1;
        }

        if (fread((void*) &vsize, sizeof(vsize), 1, kvs->fp) != 1)
        {
            return -1;
        }

        key = (char*) malloc(ksize);
        if (key == NULL)
        {
            return -1;
        }

        if (fread((void*) key, sizeof(char), ksize, kvs->fp) != ksize)
        {
            free(key);
            return -1;
        }

        if (fseek(kvs->fp, KVS_ALIGN(ksize + vsize) - ksize, SEEK_CUR) != 0)
        {
            free(key);
            return -1;
        }

        kv = kvs_allocate_kv(addr, used, ksize, vsize, key);
        if (kv == NULL)
        {
            free(key);
            return -1;
        }

        kvs_insert_kv(kvs, kv);
    }

    return 0;
}

int kvs_open(kvs_t* kvs, char* path)
{
    char* mode = NULL;

    kvs->path = path;

    if (access(path, F_OK) == 0)
    {
        mode = "r+";
    }
    else
    {
        mode = "w+";
    }

    kvs->fp = fopen(path, mode);
    if (kvs->fp == NULL)
    {
        return KVS_ERROR;
    }

    kvs->kv = kvs_allocate_kv(0, 1, 0, 0, NULL);
    if (kvs->kv == NULL)
    {
        fclose(kvs->fp);
        return KVS_ERROR;
    }

    if (kvs_load(kvs) != 0)
    {
        kvs_close(kvs);
        return KVS_ERROR;
    }

    return KVS_OK;
}

void kvs_close(kvs_t* kvs)
{
    kv_t* kv   = NULL;
    kv_t* next = NULL;

    kv = kvs->kv->next;
    while (kv != NULL)
    {
        next = kv->next;
        kvs_delete_kv(kvs, kv);
        kvs_deallocate_kv(kv);
        kv = next;
    }

    kvs_deallocate_kv(kvs->kv);
    fclose(kvs->fp);
}

int kvs_has(kvs_t* kvs, char* key)
{
    char  ksize = 0;
    kv_t* kv    = NULL;

    ksize = (char) strlen(key);

    FOR(kvs, kv)
    {
        if (kv->used && ksize == kv->ksize && strncmp(key, kv->key, ksize) == 0)
        {
            return 1;
        }
    }

    return 0;
}

int kvs_get(kvs_t* kvs, char* key, kvs_value_t* value)
{
    char  ksize = 0;
    kv_t* kv    = NULL;
    char* data  = NULL;

    ksize = (char) strlen(key);

    FOR(kvs, kv)
    {
        if (kv->used && ksize == kv->ksize && strncmp(key, kv->key, ksize) == 0)
        {
            data = kvs_read_value(kvs, kv);
            if (data == NULL)
            {
                return KVS_ERROR;
            }

            value->data = data;
            value->size = kv->vsize;

            return KVS_OK;
        }
    }

    return KVS_KEY_DOES_NOT_EXIST;
}

int kvs_set(kvs_t* kvs, char* key, kvs_value_t* value)
{
    long  addr    = 0;
    char  ksize   = 0;
    int   vsize   = 0;
    int   size    = 0;
    kv_t* kv      = NULL;
    kv_t* free_kv = NULL;
    char* new_key = NULL;

    ksize = (char) strlen(key);
    vsize = value->size;
    size  = KVS_ALIGN(ksize + vsize);

    FOR(kvs, kv)
    {
        if (kv->used)
        {
            if (ksize == kv->ksize && strncmp(key, kv->key, ksize) == 0)
            {
                return KVS_KEY_ALREADY_EXISTS;
            }
        }
        else
        {
            printf("%d\n", size);
            if (KVS_SIZE(kv) == size)
            {
                free_kv = kv;
            }
            else if (KVS_SIZE(kv) > size && free_kv == NULL)
            {
                free_kv = kv;
            }
        }
    }

    new_key = (char*) malloc(ksize+1);
    if (new_key == NULL)
    {
        return KVS_ERROR;
    }
    strncpy(new_key, key, ksize);

    if (free_kv != NULL)
    {
        if (KVS_SIZE(free_kv) > size)
        {
            kv_t* remainder = kvs_allocate_kv(free_kv->addr + KVS_HEADER_SIZE + size, 0, 0, KVS_SIZE(free_kv) - KVS_HEADER_SIZE - size, NULL);
            if (remainder == NULL)
            {
                free(new_key);
                return KVS_ERROR;
            }
            
            if (kvs_write_header(kvs->fp, remainder) != 0)
            {
                kvs_deallocate_kv(remainder);
                free(new_key);
                return KVS_ERROR;
            }
            kvs_append_kv(kvs, free_kv, remainder);
        }

        free_kv->used = 1;
        free_kv->ksize = ksize;
        free_kv->vsize = vsize;
        free_kv->key   = new_key;

        if (kvs_write_data(kvs->fp, free_kv, value->data) != 0)
        {
            return KVS_ERROR;
        }
    }
    else
    {
        kv_t* new_kv = NULL;

        if (fseek(kvs->fp, 0, SEEK_END) != 0)
        {
            free(new_key);
            return KVS_ERROR;
        }

        addr = ftell(kvs->fp);
        if (addr == -1)
        {
            free(new_key);
            return KVS_ERROR;
        }

        new_kv = kvs_allocate_kv(addr, 1, ksize, vsize, new_key);
        if (new_kv == NULL)
        {
            free(new_key);
            return KVS_ERROR;
        }

        if (kvs_write_data(kvs->fp, new_kv, value->data) != 0)
        {
            kvs_deallocate_kv(new_kv);
            return KVS_ERROR;
        }
        kvs_insert_kv(kvs, new_kv);
    }

    return KVS_OK;
}

int kvs_delete(kvs_t* kvs, char* key)
{
    char  ksize = 0;
    kv_t* kv    = NULL;
    kv_t* next  = NULL;
    kv_t* prev  = NULL;

    ksize = (char) strlen(key);

    FOR(kvs, kv)
    {
        if (kv->used && ksize == kv->ksize && strncmp(key, kv->key, ksize) == 0)
        {
            kv->used = 0;

            if (kv->key != NULL)
            {
                free(kv->key);
                kv->key = NULL;
            }
            kv->vsize = KVS_SIZE(kv);
            kv->ksize = 0;

            next = kv->next;
            prev = kv->prev;
            
            if (next && !next->used)
            {
                kv->vsize += KVS_HEADER_SIZE + KVS_SIZE(next);
                kvs_delete_kv(kvs, next);
                kvs_deallocate_kv(next);
            }

            if (prev && !prev->used)
            {
                prev->vsize += KVS_HEADER_SIZE + KVS_SIZE(kv);
                kvs_delete_kv(kvs, kv);
                kvs_deallocate_kv(kv);

                if (kvs_write_header(kvs->fp, prev) != 0)
                {
                    return KVS_ERROR;
                }
            }
            else
            {
                if (kvs_write_header(kvs->fp, kv) != 0)
                {
                    return KVS_ERROR;
                }
            }

            return KVS_OK;
        }
    }
    
    return KVS_KEY_DOES_NOT_EXIST;
}

int kvs_defragment(kvs_t* kvs)
{
    long  addr = 0;
    FILE* fp   = NULL;
    kv_t* kv   = NULL;
    kv_t* next = NULL;
    char* data = NULL;

    fp = fopen(KVS_DEFRAG_PATH, "w");
    if (fp == NULL)
    {
        return KVS_ERROR;
    }

    kv = kvs->kv->next;
    while (kv != NULL)
    {
        next = kv->next;

        if (kv->used)
        {
            addr = ftell(fp);
            if (addr == -1)
            {
                fclose(fp);
                return KVS_ERROR;
            }

            data = kvs_read_value(kvs, kv);
            if (data == NULL)
            {
                fclose(fp);
                return KVS_ERROR;
            }

            kv->addr = addr;

            if (kvs_write_data(fp, kv, data) != 0)
            {
                free(data);
                fclose(fp);
                return KVS_ERROR;
            }

            free(data);
        }
        else
        {
            kvs_delete_kv(kvs, kv);
            kvs_deallocate_kv(kv);
        }

        kv = next;
    }

    fclose(fp);

    if (rename(KVS_DEFRAG_PATH, kvs->path) != 0)
    {
        return KVS_ERROR;
    }

    fclose(kvs->fp);

    kvs->fp = fopen(kvs->path, "r+");
    if (kvs->fp == NULL)
    {
        return KVS_ERROR;
    }

    return KVS_OK;
}
