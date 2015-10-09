"""
KVS is a key-value store.

    >>> db = KVS('test.db')
    >>> db.open()
    >>> db['a'] = 'foo'
    >>> db['a']
    'foo'
    >>> del db['a']
    >>> db.defragment()

FORMAT:

    >>> KVS
    ...   [KV]*                     # The key-value pairs.

    >>> KV
    ...   meta:6                    # Metadata:
    ...     used:1                  #     Whether this slot is used or free.
    ...     ksize:1                 #     The key size (0-255B).
    ...     vsize:4                 #     The value size (0-4GB).
    ...   data:align(vsize+ksize)   # Data:
    ...     key:ksize               #     The key.
    ...     value:vsize             #     The value.
    ...     padding:...             #     Optional padding so the data size is aligned to the metadata size.

ALGORITHM:

    >>> def has(key):
    ...     for key in kvs:
    ...         if kv.used and kv.key == key:
    ...             return True
    ...     return False
    
    >>> def get(key):
    ...     for kv in kvs:
    ...         if kv.used and kv.key == key:
    ...             return kv.value
    ...     raise Error()

    >>> def set(key, value):
    ...     slot = None
    ...     size = len(key) + len(value)
    ...     # Make sure the key is not used and watch for free slots for the data.
    ...     for kv in kvs:
    ...         if kv.used and kv.key == key:
    ...             raise Error()
    ...         if kv.used:
    ...             continue
    ...         # Remember slots with a perfect size.
    ...         if kv.size == size:
    ...             slot = kv
    ...         # Remember the first slot big enough in case no slots are perfect.
    ...         if kv.size > size and not slot:
    ...             slot = kv
    ...     # If no slots are big enough, create a new one.
    ...     if not slot:
    ...         slot = KV(size, used=True)
    ...     # If the slot is too big, split its remainder to a separate free slot.
    ...     # The data size is aligned to the metadata size, so this always works.
    ...     if slot.size > size:
    ...         rest = KV(slot.size - size, used=False)
    ...         slot.size = size
    ...     slot.key   = key
    ...     slot.value = value
    ...     slot.flush()

    >>> def delete(key):
    ...     for kv in kvs:
    ...         if kv.used and kv.key == key:
    ...             kv.used = False
    ...             # If the next KV is free, merge.
    ...             if not kv.next.used:
    ...                 kv.size += kv.next.size
    ...                 kv.next.delete()
    ...             # If the prev KV is free, merge.
    ...             if not kv.prev.used:
    ...                 kv.prev.size += kv.size
    ...                 kv.delete()
    ...             return
    ...     raise Error()

    >>> def defragment():
    ...     with open(self.path + '.tmp') as file:
    ...         for kv in kvs:
    ...             if kv.used:
    ...                 file.write(kv)
    ...     os.rename(self.path + '.tmp', self.path)

"""

import os, struct, sys

meta_fmt  = '<BBI'
meta_size = struct.calcsize(meta_fmt)

def align(size):
    return size + (meta_size - size % meta_size)

def pad(data):
    return data.ljust(align(len(data)), '\x00')

class KVS:
    
    def __init__(self, path):
        self.path = path
        self._kvs = []

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def open(self):
        if os.path.exists(self.path):
            self.file = open(self.path, 'rb+')
            self._load()
        else:
            self.file = open(self.path, 'wb+')

    def close(self):
        self.file.close()

    def __contains__(self, key):
        return any(kv.used and kv.key == key for kv in self._kvs)

    def __getitem__(self, key):
        for kv in self._kvs:
            if kv.used and kv.key == key:
                return kv.value
        raise LookupError('%s does not exist' % key)

    def __setitem__(self, key, value):
        ksize, vsize, size = len(key), len(value), align(len(key) + len(value))
        slot = None
        for kv in self._kvs:
            if kv.used:
                if kv.key == key:
                    raise LookupError('%s already exists' % key)
                continue
            if kv.size == size:
                slot = kv
            elif kv.size > size and not slot:
                slot = kv
        if slot:
            slot.used = True
            if slot.size > size:
                rest = KV(
                    kvs   = self,
                    addr  = slot.addr + meta_size + size,
                    used  = False,
                    ksize = 0,
                    vsize = slot.size - meta_size - size,
                    key   = b'',
                )
                self._append_after(slot, rest)
                rest.save_meta()
            slot.ksize = ksize
            slot.vsize = vsize
            slot.key   = key
            slot.save_data(value)
            return
        self.file.seek(0, os.SEEK_END)
        slot = KV(
            kvs   = self,
            addr  = self.file.tell(),
            used  = True,
            ksize = ksize,
            vsize = vsize,
            key   = key,
        )
        self._append(slot)
        slot.save_data(value)

    def __delitem__(self, key):
        for kv in self._kvs:
            if kv.used and kv.key == key:
                kv.used = False
                if kv.next and not kv.next.used:
                    kv.vsize += meta_size + kv.next.size
                    self._remove(kv.next)
                if kv.prev and not kv.prev.used:
                    kv.prev.vsize += meta_size + kv.size
                    self._remove(kv)
                    kv.prev.save_meta()
                else:
                    kv.save_meta()
                return
        raise LookupError('%s does not exist' % key)

    def defragment(self):
        with open('.%s.defrag' % self.path, 'wb') as file:
            for kv in self._kvs:
                if kv.used:
                    addr = file.tell()
                    file.write(struct.pack(meta_fmt, kv.used, kv.ksize, kv.vsize) + pad(kv.key + kv.value))
                    kv.addr = addr
        os.rename('.%s.defrag' % self.path, self.path)
        self.file.close()
        self.file = open(self.path, 'rb+')

    def _load(self):
        while True:
            addr = self.file.tell()
            meta = self.file.read(meta_size)
            if not meta:
                break
            used, ksize, vsize = struct.unpack(meta_fmt, meta)
            key = self.file.read(ksize)
            self.file.seek(align(ksize + vsize) - ksize, os.SEEK_CUR)
            self._append(KV(self, addr, used, ksize, vsize, key))

    def _append(self, kv):
        self._append_after(self._kvs[-1], kv) if self._kvs else self._kvs.append(kv)

    def _append_after(self, kv, new):
        index = self._kvs.index(kv)
        self._kvs.insert(index+1, new)
        if kv.next:
            kv.next.prev = new
        new.next = kv.next
        kv.next = new
        new.prev = kv

    def _remove(self, kv):
        self._kvs.remove(kv)
        if kv.next:
            kv.next.prev = kv.prev
        if kv.prev:
            kv.prev.next = kv.next

class KV(object):
    
    def __init__(self, kvs, addr, used, ksize, vsize, key):
        self.kvs   = kvs
        self.addr  = addr
        self.used  = used
        self.ksize = ksize
        self.vsize = vsize
        self.key   = key
        self.prev  = None
        self.next  = None

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.key)

    @property
    def value(self):
        self.kvs.file.seek(self.addr + meta_size + self.ksize)
        return self.kvs.file.read(self.vsize)

    @property
    def size(self):
        return align(self.ksize + self.vsize)

    def save_meta(self):
        self.kvs.file.seek(self.addr)
        self.kvs.file.write(struct.pack(meta_fmt, self.used, self.ksize, self.vsize))

    def save_data(self, value):
        self.kvs.file.seek(self.addr)
        self.kvs.file.write(struct.pack(meta_fmt, self.used, self.ksize, self.vsize) + pad(self.key + value))
