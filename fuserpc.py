#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
#Added by Anirudh Subramanian begin
import xmlrpclib
import pickle
from xmlrpclib import Binary
from memcacheht import memcacheht
#Added by Anirudh Subramanian End




from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

metadata = "'"

#Added by Anirudh Subramanian to create a distributed client server in memory filesystem

"""
class DistributedServer:

    #Wrapper functions so that the main client filesystem doesnt need to know if it is a single
    #or a distributed filesytem 
    def __init__(self, urlList):
        self.currentIndex = 0
        self.datatable = []
        self.length = 10000
        self.listLength = len(urlList)
        self.count = (self.listLength)*(self.length)
        self.keyServerMapping = {}

        for url in urlList:
            self.datatable.append(xmlrpclib.ServerProxy(url))
            print self.datatable
   
    def get(self, key):     
            print "********************************************"
            print "keyServerMapping"
            print self.keyServerMapping
            print "********************************************"
            self.print_content() 
            index = self.keyServerMapping[key.data]
            return self.datatable[index].get(key)
   
    def put(self, key, value, ttl):
            if self.currentIndex == self.count: self.currentIndex = 0
            index = self.currentIndex % self.listLength
            self.keyServerMapping[key.data] = index
            returnVal = self.datatable[index].put(key, value, ttl)
            self.currentIndex = self.currentIndex + 1
            return returnVal
    
    def print_content(self):
           for i in range(self.listLength):
           	self.datatable[i].print_content()
   
"""          

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'
    #Added by Anirudh Subramanian Begin
    #Added by Anirudh Subramanian End
    """  
        Call self.store to push metadata information corresponding to a file to simpleht service
        Note that pickle.dumps on any python object causes it to stringand stored in server. When we want
        to retrieve the data we do pickle.loads again to retrieve the object from the string
    """
    def __init__(self, distributedServer):
        self.files = {}
        #self.data = xmlrpclib.ServerProxy("http://127.0.0.1:51234")
        self.data = distributedServer
        #self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        #Commented and Added by Anirudh Subramanian Begin
        """
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        """
        
        metadatadict = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        
        self.store(self.transform('/'), metadatadict)
        #Commented and Added by Anirudh Subramanian End

    """Method added by Anirudh Subramanian to transform key to have a string padding"""
    """
       The metadata is the trailing character for the key.It is kept as apostrophe so that there
       arent any collisions between the metdata keys and the data keys
    """
    def transform(self, s):
        return str(s + metadata)

    """Method added by Anirudh Subramanian to extract normal path from the string"""
    def extract(self, s):
        return s[:-1]

    """Method added by Anirudh Subramanian to Retrieve a property in metadata"""
    def retrieveDict(self, path):
	#MEmcache
        #mdict = pickle.loads(self.data.get(self.transform(path))['value'])
        mdict = self.data.get(self.transform(path))
	
	print "="*10
	print "Inside retrieveDict"
	print "path is " + self.transform(path)
	print "mdict is "
	print mdict
	print "="*10
	#MEmcache
        return mdict

    """Method added by Anirudh Subramanian to Remove a property in metadata"""
    def deleteDict(self, path):
        self.data.put(self.transform(path), {}, 0) 
        self.files.pop(self.transform(path))
    """Method added by Anirudh Subramanian to store property in metadata"""
    def storeDict(self, path, mdict):
        self.store(self.transform(path), mdict)

    def chmod(self, path, mode):
        #Commented and Added by Anirudh Subramanian Begin
        metadatadict = self.retrieveDict(path)
        metadatadict['st_mode'] &= 0770000
        metadatadict['st_mode'] |= mode
        self.storeDict(path, metadatadict)
        #Commented and Added by Anirudh Subramanian End
        return 0

    def chown(self, path, uid, gid):
        
        #Commented and Added by Anirudh Subramanian Begin
        metadatadict = self.retrieveDict(path)
        metadatadict['st_uid'] = uid
        metadatadict['st_gid'] = gid
        self.storeDict(path, metadatadict)
        #Commented and Added by Anirudh Subramanian End

    def create(self, path, mode):
        #Commented and Added by Anirudh Subramanian Begin
        print "=============================================="
        print "Inside create"
        print "=============================================="
        metadatadict = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        self.storeDict(path, metadatadict)
        self.store(path, "") 
        #Commented and Added by Anirudh Subramanian End

        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        #Commented and Added by Anirudh Subramanian Begin
        """
        if path not in self.files:
            raise FuseOSError(ENOENT)
        """
        key = self.transform(path)
        print 'key is ' + key
        try:
            self.data.get(key)
            print "===================================="
            print "INside getattr"
	    print self.retrieveDict(path)
	    mdict = self.retrieveDict(path)
	    if mdict == None:
		raise FuseOSError(ENOENT)
            print "===================================="
            return mdict
        except KeyError:
             print "In key errpr"
             raise FuseOSError(ENOENT)
        #return self.files[path]
        #Commented and Added by Anirudh Subramanian End


    def getxattr(self, path, name, position=0):
        #Commented and Added by Anirudh Subramanian Begin
        #attrs = self.files[path].get('attrs', {})
        mdict = self.retrieveDict(path)
        attrs = mdict.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR
        #Commented and Added by Anirudh Subramanian End

    def listxattr(self, path):
        #Commented and Added by Anirudh Subramanian Begin
        #attrs = self.files[path].get('attrs', {})
        mdict = self.retrieveDict(path)
        attrs = mdict.get('attrs', {})
        return attrs.keys()
        #Commented and Added by Anirudh Subramanian End

    def mkdir(self, path, mode):
        #Commented and Added by Anirudh Subramanian Begin
        """
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

        self.files['/']['st_nlink'] += 1
        """
        metadatadict = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        self.storeDict(path, metadatadict)
        mdictroot  = self.retrieveDict('/')
        mdictroot['st_nlink'] += 1
        self.storeDict('/', mdictroot)
        #Commented and Added by Anirudh Subramanian End

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        #Added and commented by Anirudh Subramanian
        #MEmcache
	#i = self.data.get(path)['value']
	i = self.data.get(path)
        #MEmcache
	#arr = pickle.loads(i.data)['value']
        #arr = pickle.loads(i.data)['value']
        #arr = bytearray(arr)
        print "========================================================="
        print i
        print "========================================================="
        #j = pickle.loads(i.data)['value']
        print "Read output is =========================================="
        #print pickle.loads(i.data)['value']
        r = i
        print "========================================================="
        return r[offset:offset + size] 
        #return self.data[path][offset:offset + size]
        #Added and commented by Anirudh Subramanian
    """
     Method modified to retrieve file names. The x[-1] == metadata is to check for the trailing character and make sure it
     is a directory or a  file and not just a file
    """
    def readdir(self, path, fh):
        #Added and commented by Anirudh Subramanian
        #return ['.', '..'] + [x[1:] for x in self.files if x != '/']
        return ['.', '..'] + [x[1:-1] for x in self.files if x != self.transform("/") and x[-1] == metadata]
        #Added and commented by Anirudh Subramanian

    def readlink(self, path):
        return self.data.get(path)
	#MEmcache
	#return pickle.loads(self.data.get(path)['value'])
	#MEmcache
    def removexattr(self, path, name):
        #Added and commented by Anirudh Subramanian
        
        mdict = self.retrieveDict(path)
        attrs = mdict.get('attrs', {})
        """
        attrs = self.files[path].get('attrs', {})
        """
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR
        #Added and commented by Anirudh Subramanian

    def rename(self, old, new):
        """
        if (self.data.get(Binary(path))):
           i = self.data.get(Binary(path))['value']
           self.store(path, pickle.dumps(pickle.loads(i.data)[:length]))
        
        """
        #Added and commented by Anirudh Subramanian
        #self.files[new] = self.files.pop(old)
        mdict = self.retrieveDict(old)
	#MEmcache
        #data1  = self.data.get(old)['value']
	data1  = self.data.get(old)
	#MEmcache
        #print "mdict is " + pickle.loads(mdict)
        #print "data  is " + data1.data
        self.deleteDict(old)
        self.storeDict(new, mdict)
        self.store(new, data1) 
        #Added and commented by Anirudh Subramanian

    def rmdir(self, path):
        #Added and commented by Anirudh Subramanian
        """
        self.files.pop(path)
        
        self.files['/']['st_nlink'] -= 1
        """
        #self.data.pop(self.transform(path))
        self.deleteDict(path)
        mdict = self.retrieveDict('/') 
        mdict['st_nlink'] -= 1
        #Added and commented by Anirudh Subramanian

    def setxattr(self, path, name, value, options, position=0):
        #Added and commented by Anirudh Subramanian
        # Ignore options
        mdict = self.retrieveDict(path)
        print "inside setxattr "
	print "mdict is "
	print mdict 
	attrs = mdict.setdefault('attrs', {})
        #attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value
        #Added and commented by Anirudh Subramanian

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    #Added by Anirudh Subramanian for storing using simple ht service begin
    def store(self, keyname, i):
        self.data.put(keyname, i, 20000)
        self.files[keyname] = 0
    #Added by Anirudh Subramanian for storing using simple ht service end

    def symlink(self, target, source):
        metadatadict = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))
        self.storeDict(target, metadatadict)
        #Commented and Added by Anirudh Subramanian Begin
        #self.data[target] = source
        print "inside symlink . source is" + source
        self.store(target, source) 
        
        #Commented and Added by Anirudh Subramanian End


    def truncate(self, path, length, fh=None):
        print "Inside truncate ===================================================================="
        if (self.data.get(path)):
           print "Inside truncate ================================================================="
           #MEmcache
	   #i = self.data.get(path)['value']
	   i = self.data.get(path)
	   #MEmcache
           j = self.data.get(path)
           print i
           print "================================================================="
           self.store(path, (i)[:length])
           print "Inside truncate ================================================================="
             
        else:
           #self.store(path, pickle.dumps(pickle.loads(Binary("").data)))
           self.store(path, "")
        mdict = self.retrieveDict(path)
        mdict['st_size'] = length
        self.storeDict(path, mdict)
        #Commented and Added by Anirudh Subramanian Begin
        #self.data[path] = self.data[path][:length]
        #self.files[path]['st_size'] = length
        #Commented and Added by Anirudh Subramanian End

    def unlink(self, path):
        #Commented and Added by Anirudh Subramanian Begin
        #self.data.pop(self.transform(path))
        self.deleteDict(path)
        #Commented and Added by Anirudh Subramanian End

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        mdict = self.retrieveDict(path)
        mdict['st_atime'] = atime
        mdict['st_mtime'] = mtime
        self.storeDict(path, mdict)
        #self.files[path]['st_atime'] = atime
        #self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        #Commented and added by Anirudh Subramanian Begin
        #MEmcache
	#i = self.data.get(path)['value']
	i = self.data.get(path)
        #MEmcache
        print 'i is ' + i
        if i:
           #MEmcache
           #j = self.data.get(path)['value']
           j = self.data.get(path)
           r = j
           finaldata = r[:offset] + data
        else:
           finaldata = data
        lengthData = len(finaldata)
        #self.data[path] = self.data[path][:offset] + data
        self.store(path, finaldata)
        mdict = self.retrieveDict(path)
        mdict['st_size'] = lengthData      
        #self.files[path]['st_size'] = lengthData
        self.storeDict(path, mdict)
        #Commented and added by Anirudh Subramanian End
        return len(data)


if __name__ == '__main__':
    if len(argv) < 3:
        print('usage: %s <mountpoint> <serverurl1> .. <serverurlN>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    #Modified the distributed server to memcache client server
    mht = memcacheht(argv[2:])
    #db.put(Binary('hello'), Binary('random'), 20000)
    #print db.get(Binary('hello'))
    #Modified by Anirudh Subramanian to enable logging Begin
    fuse = FUSE(Memory(mht), argv[1], foreground=True, debug=True)
    #Modified by Anirudh Subramanian to enable logging End
