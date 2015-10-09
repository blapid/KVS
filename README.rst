Build
-----

- The Python POC is in the ``poc/`` directory.
- The header files are in the ``inc/`` directory.
- The source files are in the ``src/`` directory.
- To build the project, run:

  .. code:: sh
   
     $ make [all|static|shared|demo]

- The object files are in the ``obj/`` directory.
- The static library, shared library, and demo executable are in the ``exe/`` directory.
- To clean the project, run:

  .. code :: sh

     $ make clean

- See the ``Makefile`` for toolchain configurations.

Usage
-----

.. code :: c

   #include "kvs.h"

   main()
   {
       kvs_t kvs = KVS_INIT;
       kvs_value_t v;

       kvs_open(&kvs, "test.kvs");

       kvs_has(&kvs, "a");

       kvs_get(&kvs, "a", &v);

       v.size = 3;
       v.data = "foo";
       kvs_set(&kvs, "a", &v);

       kvs_delete(&kvs, "a");

       kvs_defragment(&kvs);

       kvs_close(&kvs);
   }

Note that value data returned by ``kvs_get`` needs to be freed.

.. code :: c

    #include "kvs.h"

    main()
    {
        kvs_t kvs = KVS_INIT;
        kvs_value_t v;

        kvs_open(&kvs, "test.kvs");

        kvs_get(&kvs, "a", &v);
        
        // ...

        free(v->data);

        kvs_close(&kvs);
    }
