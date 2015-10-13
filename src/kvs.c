#include "kvs.h"

#include <stdio.h>  // fopen, fclose, fseek, ftell, feof, rename, SEEK_SET, SEEK_END, SEEK_CUR
#include <stdlib.h> // malloc, memset
#include <string.h> // strlen, strncmp
#include <unistd.h> // access, F_OK, ftruncate
#include <sys/stat.h> // stat, struct stat
#include <fcntl.h> // open, O_RDWR
#include <sys/mman.h> //mmap, PROT_READ, PROT_WRITE, MAP_FAILED, MAP_SHARED

#define KVS_HEADER_SIZE (sizeof(kv_t))
#define KVS_ALIGN(n)    (((n) % KVS_HEADER_SIZE == 0) ? (n) : (n) + (KVS_HEADER_SIZE - ((n) % KVS_HEADER_SIZE)))
#define KVS_PADDING(n)  (KVS_ALIGN(n) - (n))
#define KVS_SIZE(kv)    (KVS_ALIGN((kv)->ksize + (kv)->vsize))
#define FOR(kvs, i)     for (i = kvs->kv_index; i != NULL; i = i->next)

inline kv_t *kvs_kv_from_index(kvs_t *kvs, kv_index_t *kv_index)
{
    return (kv_t *)(kvs->base + kv_index->offset);
}

inline char kvs_used_from_index(kvs_t *kvs, kv_index_t *kv_index)
{
    return kvs_kv_from_index(kvs, kv_index)->used;
}

inline char *kvs_key_from_index(kvs_t *kvs, kv_index_t *kv_index)
{
    return (char *)(kvs->base + kv_index->offset + sizeof(kv_t));
}

inline char kvs_ksize_from_index(kvs_t *kvs, kv_index_t *kv_index)
{
    return kvs_kv_from_index(kvs, kv_index)->ksize;
}

inline char *kvs_value_from_index(kvs_t *kvs, kv_index_t *kv_index)
{
    return (char *)(kvs->base + kv_index->offset +
        sizeof(kv_t) + kvs_ksize_from_index(kvs, kv_index));
}

inline int kvs_vsize_from_index(kvs_t *kvs, kv_index_t *kv_index)
{
    return kvs_kv_from_index(kvs, kv_index)->vsize;
}

inline int kvs_size_from_index(kvs_t *kvs, kv_index_t *kv_index)
{
    return KVS_HEADER_SIZE + KVS_ALIGN(kvs_ksize_from_index(kvs, kv_index)
        + kvs_vsize_from_index(kvs, kv_index));
}

static kv_index_t* kvs_allocate_kv_index(long offset)
{
    kv_index_t* kv = NULL;

    kv = (kv_index_t*) malloc(sizeof(kv_index_t));
    if (kv == NULL)
    {
        return NULL;
    }

    kv->offset = offset;
    kv->prev  = NULL;
    kv->next  = NULL;

    return kv;
}

static void kvs_deallocate_kv_index(kv_index_t* kv)
{
    free(kv);
}

static void kvs_append_kv_index(kvs_t* kvs, kv_index_t* kv, kv_index_t* new_kv)
{
    if (kv->next != NULL)
    {
        kv->next->prev = new_kv;
    }
    new_kv->next = kv->next;
    kv->next = new_kv;
    new_kv->prev = kv;

    if (kvs->kv_index->prev == kv)
    {
        kvs->kv_index->prev = new_kv;
    }
}

static void kvs_insert_kv_index(kvs_t* kvs, kv_index_t* kv)
{
	if (kvs->kv_index == NULL)
	{
		kvs->kv_index = kv;
		kv->prev = kv;
	}
	else if (kvs->kv_index->prev == NULL)
	{
		kvs->kv_index->next = kv;
		kv->prev = kvs->kv_index;
		kvs->kv_index->prev = kv;
	}
	else
	{
		kvs_append_kv_index(kvs, kvs->kv_index->prev, kv);
	}
}

static void kvs_delete_kv_index(kvs_t* kvs, kv_index_t* kv)
{
    if (kv->next != NULL)
    {
        kv->next->prev = kv->prev;
    }
    if (kvs->kv_index->prev == kv)
    {
        kvs->kv_index->prev = kv->prev;
    }
    if (kvs->kv_index == kv)
    {
        kvs->kv_index = kv->next;
    }
    else
    {
        kv->prev->next = kv->next;
    }
}


static int kvs_load(kvs_t* kvs)
{
    long offset = 0;

    while (1)
    {
        if (offset >= kvs->map_size)
        {
            break;
        }
        kv_index_t *kv_index = kvs_allocate_kv_index(offset);
        kv_t *kv = kvs_kv_from_index(kvs, kv_index);

        kvs_insert_kv_index(kvs, kv_index);

        offset += sizeof(kv_t) + KVS_ALIGN(kv->ksize + kv->vsize);
    }

    return 0;
}

int kvs_open(kvs_t* kvs, char* path)
{
    struct stat st = {0};

    kvs->path = path;

    kvs->fd = open(path, O_RDWR);
    if (kvs->fd == -1)
    {
        return KVS_ERROR;
    }

    if (fstat(kvs->fd, &st) == -1)
    {
        close(kvs->fd);
        return KVS_ERROR;
    }

    if (st.st_size > 0)
    {
        //TODO: Should probably page align the size
        kvs->base = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, kvs->fd, 0);

        if (kvs->base == MAP_FAILED)
        {
            close(kvs->fd);
            return KVS_ERROR;
        }

        kvs->map_size = st.st_size;
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
    kv_index_t* kv_index   = NULL;
    kv_index_t* next_index = NULL;

    kv_index = kvs->kv_index;
    while (kv_index != NULL)
    {
        next_index = kv_index->next;
        kvs_delete_kv_index(kvs, kv_index);
        kvs_deallocate_kv_index(kv_index);
        kv_index = next_index;
    }

    munmap(kvs->base, kvs->map_size);
    kvs->base = 0;
    close(kvs->fd);
    kvs->fd = -1;
}

int kvs_has(kvs_t* kvs, char* key)
{
    char  ksize          = 0;
    kv_index_t* kv_index = NULL;
    kv_t* kv             = NULL;

    ksize = (char) strlen(key);

    FOR(kvs, kv_index)
    {
        kv = kvs_kv_from_index(kvs, kv_index);
        if (kv->used && ksize == kv->ksize && strncmp(key, kvs_key_from_index(kvs, kv_index), ksize) == 0)
        {
            return 1;
        }
    }

    return 0;
}

int kvs_get(kvs_t* kvs, char* key, kvs_value_t* value)
{
    char  ksize          = 0;
    kv_index_t* kv_index = NULL;
    kv_t* kv             = NULL;

    ksize = (char) strlen(key);

    FOR(kvs, kv_index)
    {
        kv = kvs_kv_from_index(kvs, kv_index);
        if (kv->used && ksize == kv->ksize && strncmp(key, kvs_key_from_index(kvs, kv_index), ksize) == 0)
        {
            value->data = kvs_value_from_index(kvs, kv_index);
            value->size = kv->vsize;

            return KVS_OK;
        }
    }

    return KVS_KEY_DOES_NOT_EXIST;
}

int kvs_set(kvs_t* kvs, char* key, kvs_value_t* value)
{
    char  ksize                = 0;
    int   vsize                = 0;
    int   size                 = 0;
    kv_index_t* kv_index       = NULL;
    kv_index_t* free_kv_index  = NULL;
    kv_t* kv                   = NULL;
    kv_t* free_kv              = NULL;
    char* new_key              = NULL;

    ksize = (char) strlen(key);
    vsize = value->size;
    size  = KVS_ALIGN(ksize + vsize);

    FOR(kvs, kv_index)
    {
        kv = kvs_kv_from_index(kvs, kv_index);
        if (kv->used)
        {
            if (ksize == kv->ksize && strncmp(key, kvs_key_from_index(kvs, kv_index), ksize) == 0)
            {
                return KVS_KEY_ALREADY_EXISTS;
            }
        }
        else
        {
            if (KVS_SIZE(kv) == size)
            {
                free_kv_index = kv_index;
            }
            else if (KVS_SIZE(kv) > size && free_kv_index == NULL)
            {
                free_kv_index = kv_index;
            }
        }
    }

    if (free_kv_index != NULL)
    {
        free_kv = kvs_kv_from_index(kvs, free_kv_index);
        if (KVS_SIZE(free_kv) > size)
        {
            kv_t *remainder = NULL;
            kv_index_t *remainder_index = kvs_allocate_kv_index(free_kv_index->offset + KVS_HEADER_SIZE + size);

            if (remainder_index == NULL)
            {
                free(new_key);
                return KVS_ERROR;
            }

            remainder = kvs_kv_from_index(kvs, remainder_index);
            remainder->used = 0;
            remainder->ksize = 0;
            remainder->vsize = KVS_SIZE(free_kv) - KVS_HEADER_SIZE - size;

            kvs_append_kv_index(kvs, free_kv_index, remainder_index);
        }

        free_kv->used = 1;
        free_kv->ksize = ksize;
        free_kv->vsize = vsize;
        strncpy(kvs_key_from_index(kvs, free_kv_index), key, ksize);
        memcpy(kvs_value_from_index(kvs, free_kv_index), value->data, vsize);
    }
    else
    {
        long new_offset = 0;
        kv_index_t *new_kv_index = NULL;
        kv_t* new_kv = NULL;
        //TODO: Should probably page align the size. And move this to its own function
        int new_size = kvs->map_size + KVS_HEADER_SIZE + KVS_ALIGN(ksize + vsize);
        void *new_base = NULL;
        //XXX: This can also decrease the size of the file if used after defragment. Will have to consider this in the future.
        if (ftruncate(kvs->fd, new_size) != 0)
        {
            return KVS_ERROR;
        }
        //TODO: In linux we can use mremap. For now and for portability
        //      (I use OSX) we'll do this the dirty way
        new_base = mmap(kvs->base, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, kvs->fd, 0);
        if (new_base == MAP_FAILED)
        {
            return KVS_ERROR;
        }
        /*
         I think in most cases the kernel should follow our hint and avoid
         the extra work. However, we should plan for the worst.
        */
        if (kvs->base != new_base)
        {
            munmap(kvs->base, kvs->map_size);
            kvs->base = new_base;
        }
        kvs->map_size = new_size;
        if (kvs->kv_index != NULL)
        {
            new_offset = kvs->kv_index->prev->offset +
                kvs_size_from_index(kvs, kvs->kv_index->prev);
        }
        else
        {
            new_offset = 0;
        }
        new_kv_index = kvs_allocate_kv_index(new_offset);
        if (new_kv_index == NULL)
        {
            return KVS_ERROR;
        }
        new_kv = kvs_kv_from_index(kvs, new_kv_index);
        new_kv->used = 1;
        new_kv->ksize = ksize;
        new_kv->vsize = vsize;
        strncpy(kvs_key_from_index(kvs, new_kv_index), key, ksize);
        memcpy(kvs_value_from_index(kvs, new_kv_index), value->data, vsize);

        kvs_insert_kv_index(kvs, new_kv_index);
    }

    return KVS_OK;
}

int kvs_delete(kvs_t* kvs, char* key)
{
    char  ksize          = 0;
    kv_index_t* next     = NULL;
    kv_index_t* prev     = NULL;
    kv_index_t* kv_index = NULL;
    kv_t* kv             = NULL;

    ksize = (char) strlen(key);

    FOR(kvs, kv_index)
    {
        kv = kvs_kv_from_index(kvs, kv_index);
        if (kv->used && ksize == kv->ksize && strncmp(key, kvs_key_from_index(kvs, kv_index), ksize) == 0)
        {
            kv->used = 0;
            kv->vsize = KVS_SIZE(kv);
            kv->ksize = 0;

            next = kv_index->next;
            prev = kv_index->prev;

            if (next && !kvs_used_from_index(kvs, next))
            {
                kv->vsize += kvs_size_from_index(kvs, next);
                kvs_delete_kv_index(kvs, next);
                kvs_deallocate_kv_index(next);
            }

            if (prev && !kvs_used_from_index(kvs, prev))
            {
                kvs_kv_from_index(kvs, prev)->vsize += kvs_size_from_index(kvs, kv_index);
                kvs_delete_kv_index(kvs, kv_index);
                kvs_deallocate_kv_index(kv_index);
            }

            return KVS_OK;
        }
    }

    return KVS_KEY_DOES_NOT_EXIST;
}

int kvs_defragment(kvs_t* kvs)
{
    long  offset           = 0;
    kv_index_t* kv_index   = NULL;
    kv_index_t* next_index = NULL;
    kv_t* kv               = NULL;

    kv_index = kvs->kv_index->next;
    while (kv_index != NULL)
    {
        next_index = kv_index->next;
        kv = kvs_kv_from_index(kvs, kv_index);

        if (kv->used)
        {
            if (kv_index->offset != offset)
            {
                memcpy(kvs->base + offset, kvs->base + kv_index->offset,
                    kvs_size_from_index(kvs, kv_index));
                kv_index->offset = offset;
            }
        }
        else
        {
            kvs_delete_kv_index(kvs, kv_index);
            kvs_deallocate_kv_index(kv_index);
        }

        offset += kvs_size_from_index(kvs, kv_index);
        kv_index = next_index;
    }

    return KVS_OK;
}
