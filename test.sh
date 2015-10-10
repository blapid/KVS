quit()
{
    hexdump  test.kvs
    rm test.kvs 2> /dev/null
    exit
}

has()
{
    exe/demo test.kvs has $@
}

get()
{
    exe/demo test.kvs get $@
}

set()
{
    exe/demo test.kvs set $@
}

delete()
{
    exe/demo test.kvs delete $@
}

defragment()
{
    exe/demo test.kvs defragment
}

if ! make
then
    exit
fi

echo "Batch #1. Expect: No key, false, OK, foo"
has a
get a
set a foo
get a

echo "Batch #2. Expect: false, no key, OK, bar"
has b
get b
set b bar
get b

echo "Batch #3. Expect: OK, OK, OK, OK"
set c foobar
delete a
delete b
defragment

quit
