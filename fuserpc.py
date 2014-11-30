# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from sets import Set
from memcacheht import memcacheht

if not hasattr(__builtins__, 'bytes'):
    bytes = str
        
           

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self, distributedServer):
        self.files = distributedServer
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2, files_folders=Set(['/']))


    def chmod(self, path, mode):
        ht = self.files[path]
        ht['st_mode'] &= 0770000
        ht['st_mode'] |= mode
        self.files[path] = ht
        #self.files[path]['st_mode'] &= 0770000
        #self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        ht = self.files[path]
        ht['st_uid'] = uid
        ht['st_gid'] = gid
        self.files[path] = ht

    def create(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), files_folders = '')
        ht = self.files['/']
        ht['files_folders'].add(path)
        self.files['/'] = ht
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        print path
        ht = self.files['/']
        print 'filesfolders ='*10 + 'filesfolders'
        print ht['files_folders']
        print 'filesfolders ='*10 + 'filesfolders'
        if path not in self.files['/']['files_folders']:
            raise FuseOSError(ENOENT)

        return self.files[path]

    def getxattr(self, path, name, position=0):
        attrs = self.files[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), files_folders=Set())
        ht = self.files['/']
        ht['st_nlink'] += 1
        ht['files_folders'].add(path)
        self.files['/'] = ht

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        ht = self.files[path]
        if 'files_folders' in ht:
            print "read="*10 + "read"
            print ht['files_folders'][offset:offset + size]
            print "read="*10 + "read"
            return ht['files_folders'][offset:offset + size]
        return None

    def readdir(self, path, fh):
        return ['.', '..'] + [x[1:] for x in self.files['/']['files_folders'] if x != '/']

    def readlink(self, path):
        return self.files[path]['files_folders']

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        temp = self.files[old]
        self.files[new] = temp
        del self.files[old]
        #self.files[new] = self.files.pop(old)
        ht = self.files['/']
        ht['files_folders'].add(new)
        ht['files_folders'].remove(old)
        self.files['/'] = ht
        

    def rmdir(self, path):
        #self.files.pop(path)
        del self.files[path]
        ht = self.files['/']
        ht['st_nlink'] -= 1
        ht['files_folders'].remove(path)
        self.files['/'] = ht

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        ht = self.files[path]
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value
        ht['attrs'] = attrs
        self.files[path] = ht

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source), files_folders=source)
        ht = self.files['/']
        ht['files_folders'].add(target)
        self.files['/'] = ht

    def truncate(self, path, length, fh=None):
        ht = self.files[path]
        if 'files_folders' in ht:
            ht['files_folders'] = ht['files_folders'][:length]
        ht['st_size'] = length
        self.files[path] = ht

    def unlink(self, path):
        #self.files.pop(path)
        ht = self.files['/']
        ht['files_folders'].remove(path)
        self.files['/'] = ht
        del self.files[path]

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        """
        self.data[path] = self.data[path][:offset] + data
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)
        """
        print "="*20
        print "data is " + data
        print "="*20
        ht = self.files[path]
        if len(ht['files_folders']) > (len(data) + offset):
            print 'Inside if'
            ht['files_folders'] = ht['files_folders'][:offset] + data + ht['files_folders'][offset:]
        else:
            print 'Inside else'
            ht['files_folders'] = ht['files_folders'][:offset] + data
        print "Inside write="*10 + "Inside write"
        print ht['files_folders']
        ht['st_size'] = len(ht['files_folders'])
        print "Inside write="*10 + "Inside write"
       
        self.files[path] = ht 
        return len(data)


if __name__ == '__main__':
    if len(argv) < 3:
        print('usage: %s <mountpoint> <serverurl1> .. <serverurlN>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    #Modified the distributed server to memcache client server
    mht = memcacheht(argv[2:])
    mht[1] = "qwdw"
    print mht[1]
    del mht[1]
    #db.put(Binary('hello'), Binary('random'), 20000)
    #print db.get(Binary('hello'))
    #Modified by Anirudh Subramanian to enable logging Begin
    fuse = FUSE(Memory(mht), argv[1], foreground=True, debug=True)
    #Modified by Anirudh Subramanian to enable logging End
