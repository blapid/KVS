quit()
{
    hd test.kvs
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

has a
get a
set a foo
get a

has b
get b
set b bar
get b

set c foobar
delete a
delete b
defragment

quit
