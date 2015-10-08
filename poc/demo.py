#!/usr/bin/env python

import inspect, kvs, sys, traceback

def has(db, key):
    return key in db

def get(db, key):
    return db[key]

def set(db, key, value):
    db[key] = value
    return 'OK'

def delete(db, key):
    del db[key]
    return 'OK'

def defragment(db):
    db.defragment()
    return 'OK'

commands = has, get, set, delete, defragment

def usage():
    print('USAGE: %s <path> (has <key> | get <key> | set <key> <value> | delete <key> | <defragment>)')
    sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
    path, name = sys.argv[1:3]
    with kvs.KVS(path) as db:
        for command in commands:
            if command.__name__ == name and len(sys.argv) >= 3 + len(inspect.getargspec(command).args) - 1:
                try:
                    print(command(db, *sys.argv[3:]))
                except Exception as error:
                    print('ERROR: %s' % error)
                break
        else:
            usage()
