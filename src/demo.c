#include "kvs.h"

#include <stdio.h>  // printf, perror
#include <string.h> // strcmp, strlen

int main(int argc, char* argv[])
{
    int   ret = 0;
    kvs_t kvs = KVS_INIT;

    if ((argc < 3)
        || (strcmp(argv[2], "has")        == 0 && argc != 4)
        || (strcmp(argv[2], "get")        == 0 && argc != 4)
        || (strcmp(argv[2], "set")        == 0 && argc != 5)
        || (strcmp(argv[2], "delete")     == 0 && argc != 4)
        || (strcmp(argv[2], "defragment") == 0 && argc != 3)
    )
    {
        printf("USAGE: %s <path> (has <key> | get <key> | set <key> <value> | delete <key> | defragment)\n", argv[0]);
        return -1;
    }

    if (kvs_open(&kvs, argv[1]) != KVS_OK)
    {
        perror("kvs_open");
        return -1;
    }

    if (strcmp(argv[2], "has") == 0)
    {
        if (kvs_has(&kvs, argv[3]))
        {
            printf("True.\n");
        }
        else
        {
            printf("False.\n");
        }
    }
    else if (strcmp(argv[2], "get") == 0)
    {
        kvs_value_t value;

        ret = kvs_get(&kvs, argv[3], &value);
        if (ret == KVS_OK)
        {
            printf("%s\n", value.data);
        }
        else if (ret == KVS_ERROR)
        {
            perror("kvs_get");
        }
        else if (ret == KVS_KEY_DOES_NOT_EXIST)
        {
            printf("Key %s does not exist.\n", argv[3]);
        }
    }
    else if (strcmp(argv[2], "set") == 0)
    {
        kvs_value_t value;
        value.data = argv[4];
        value.size = strlen(argv[4]);

        ret = kvs_set(&kvs, argv[3], &value);
        if (ret == KVS_OK)
        {
            printf("OK.\n");
        }
        else if (ret == KVS_ERROR)
        {
            perror("kvs_set");
        }
        else if (ret == KVS_KEY_ALREADY_EXISTS)
        {
            printf("Key %s already exists.\n", argv[3]);
        }
    }
    else if (strcmp(argv[2], "delete") == 0)
    {
        ret = kvs_delete(&kvs, argv[3]);
        if (ret == KVS_OK)
        {
            printf("OK.\n");
        }
        else if (ret == KVS_ERROR)
        {
            perror("kvs_delete");
        }
        else if (ret == KVS_KEY_DOES_NOT_EXIST)
        {
            printf("Key %s does not exist.\n", argv[3]);
        }
    }
    else if (strcmp(argv[2], "defragment") == 0)
    {
        if (kvs_defragment(&kvs) != KVS_OK)
        {
            perror("kvs_defragment");
        }
        else
        {
            printf("OK.\n");
        }
    }

    kvs_close(&kvs);

    return 0;
}
