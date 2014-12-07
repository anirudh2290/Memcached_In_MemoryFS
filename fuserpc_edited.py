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
   
    """
	transform extracts the parent path from the current path
	So if current path is /mtr/tp/tp1 parent path will be /mtr/tp
    """ 

    def transform(self,s):
        if s == '/':
            return '/'
        pindex = len(s) - 1 #Modified for hierarchical FS
        hiepath = ''
        while pindex > 0: #Modified for hierarchical FS
            if s[pindex] == '/': #Modified for hierarchical FS
                break #Modified for hierarchical FS
            pindex = pindex - 1 #Modified for hierarchical FS
        hiepath = s[:pindex] #Modified for hierarchical FS
        if hiepath == '':
            hiepath = '/'        
        return hiepath


    def ltransform(self,s):
        plindex = len(s) - 1
        while plindex > 0:
            if s[plindex] == '/':
                break
            plindex = plindex - 1
        hielpath = s[(plindex)+1:len(s)]
        return hielpath

    """
	ltransform extracts current folder or filename from full path 
	name that goes into the key. Therefore if current path is
	/mtr/tp/tp1 the output will be tp1
    """
    def ltransformparent(self, s, parent):
        if parent == '/':
            return s[1:]        
        sx = str(s)
        px = str(parent) + '/'
        if(sx.startswith(px)):
            return s[len(px):]

    """
	Passed in the memcacheht object which will be the backend
	memcached hashtable that will be used
		
    """	

    def __init__(self, distributedServer):
        self.files = distributedServer
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        #self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2, files_folders=Set(['/']))
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now, st_mtime=now, st_atime=now, st_nlink=2, files_folders=Set(['/']), hpath = '/')  #Modified for hierarchical FS

    """	
	chmod retrieves the hash table and modifies the mode for the files
    """

    def chmod(self, path, mode):
        ht = self.files[path]
        ht['st_mode'] &= 0770000
        ht['st_mode'] |= mode
        self.files[path] = ht
        #self.files[path]['st_mode'] &= 0770000
        #self.files[path]['st_mode'] |= mode
        return 0
    
    """
	chown retrieves the hash table and changes the user id and group id and writes
	it back to the hash table
    """
    def chown(self, path, uid, gid):
        ht = self.files[path]
        ht['st_uid'] = uid
        ht['st_gid'] = gid
        self.files[path] = ht
     
    """
	creates the key value pair for the new file. The value will be the metadata dict
	and the files_folders key inside the value which stores contents will be initially
	blank
    """

    def create(self, path, mode):
        #self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
        #                        st_size=0, st_ctime=time(), st_mtime=time(),
        #                        st_atime=time(), files_folders = '')
       
        hp = self.transform(path) 
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), files_folders = '',hpath = hp) #Modified for hierarchical FS
        #ht = self.files['/']

        ht = self.files[hp] #Modified for hierarchical FS
        ht['files_folders'].add(path)
        #self.files['/'] = ht
        self.files[hp] = ht #Modified for hierarchical FS
        self.fd += 1
        return self.fd
    
    """
	getattr retrieves the dict and retrieves the attr key inside the 
	value
    """
    	
    def getattr(self, path, fh=None):
        hpath = self.transform(path)
        ht = self.files[hpath]
        if path not in self.files[hpath]['files_folders']:
            raise FuseOSError(ENOENT)

        return self.files[path]

    """
	getxattr retrieves the custom defined attr which retrieves the value corresponding to the key - name
    """
    def getxattr(self, path, name, position=0):
        attrs = self.files[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    """
	lists all custom defined attributes
    """
    def listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    """
	mkdir creates a dict value corresponding to the key path.
	modifications made to the link numbers and files_folders
	of the parent path
    """
    def mkdir(self, path, mode):
        #self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
        #                        st_size=0, st_ctime=time(), st_mtime=time(),
        #                        st_atime=time(e, files_folders=Set())         
        hp = self.transform(path)
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), files_folders=Set(), hpath = hp) #Modified for hierarchical FS
        #ht = self.files['/']
         
        ht = self.files[hp] #Modified for hierarchical FS
        ht['st_nlink'] += 1
        ht['files_folders'].add(path)
        ht['files_folders'].add(hp)
        #self.files['/'] = ht 
        self.files[hp] = ht #Modified for hierarchical FS
        hpp = self.transform(hp)
        ht2 = self.files[hpp]
        

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    """
	reads the file
    """
    def read(self, path, size, offset, fh):
        ht = self.files[path]
        if 'files_folders' in ht:
            return ht['files_folders'][offset:offset + size]
        return None

    """
        traverses through the files_folders to read contents of the folder
    """

    def readdir(self, path, fh):
        #return ['.', '..'] + [x[1:] for x in self.files['/']['files_folders'] if x != '/']
        hp = self.transform(path)
        #return ['.', '..'] + [x[1:] for x in self.files[hp]['files_folders'] if x != '/' or x != hp ] #Modified for hierarchical FS 
        return ['.', '..'] + [self.ltransformparent(x, path) for x in self.files[path]['files_folders'] if (self.ltransformparent(x, path) != '' and self.ltransformparent(x, path) != None)] #Modified for hierarchical FS 


    

    def readlink(self, path):
        return self.files[path]['files_folders']

    """
	Removes custom defined attributes.
	removes attr corresponding to key name in attrs dict inside the metadata
	
    """
    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})
        try:
	    ht = self.files[path]
            del attrs[name]
	    ht['attrs'] = attrs
	    self.files[path] = ht	
        except KeyError:
            pass        # Should return ENOATTR

    """
	renames files_folders associates files_folders with new and
	removes old
    """

    def rename(self, old, new):
        temp = self.files[old]
        hp = self.transform(old)
        self.files[new] = temp
        del self.files[old]
        #self.files[new] = self.files.pop(old)
        ht = self.files[hp]
        new1 = self.transform(new)
        htnew1 = self.files[new1]
        htnew1['files_folders'].add(new)
        ht['files_folders'].remove(old)
        self.files[hp] = ht
        self.files[new1] = htnew1

    """
	Removes the folder corresponding to the path
	by removing links from parent path and then
	deleting the key from the hash table
    """

    def rmdir(self, path):
        #self.files.pop(path)
        hpath = self.transform(path)
        del self.files[path]
        #ht = self.files['/']
        ht = self.files[hpath] #Modified for hierarchical FS 
        ht['st_nlink'] -= 1
        ht['files_folders'].remove(path)
        #self.files['/'] = ht
        self.files[hpath] = ht #Modified for hierarchical FS 


    """
	sets custom defined attribute corresponding to the key "name"
    """

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        ht = self.files[path]
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value
        ht['attrs'] = attrs
        self.files[path] = ht

    """
	outputs stats for the directory including size no of links,
	permissions etc. 
    """
    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    """
	symlink copies the files_folders of the source into the new 
	metadata of the target
    """
    def symlink(self, target, source):
        'creates a symlink `target -> source` (e.g. ln -s source target)'
	hp = self.transform(target)
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source), files_folders=(source), hpath=hp)
		
	ht = self.files[hp]
	ht['files_folders'].add(target)
	self.files[hp] = ht
	
	"""
	hpath = self.transform(source)
        ht = self.files[hpath]
        target1 = self.transform(target)
        httarget1 = self.files[hpath]
        ht['files_folders'].add(source)
        httarget1['files_folders'].add(target)
        self.files[hpath] = ht
        self.files[httarget1] = httarget1
	"""

    """
	truncates upto length of a file
    """
    def truncate(self, path, length, fh=None):
        ht = self.files[path]
        if 'files_folders' in ht:
            ht['files_folders'] = ht['files_folders'][:length]
        ht['st_size'] = length
        self.files[path] = ht

    """
	unlink key value corresponding to the path
    """

    def unlink(self, path):
        #self.files.pop(path)
        hpath = self.transform(path)
        ht = self.files[hpath]
        ht['files_folders'].remove(path)
        self.files[hpath] = ht
        del self.files[path]

    """
	Modifies the atime and mtime  corresponding to the
	path key 
    """
    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    """
	writes the data to a file by modifying the value corresponding to files_folders
    """
    
    def write(self, path, data, offset, fh):
        """
        self.data[path] = self.data[path][:offset] + data
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)
        """
        ht = self.files[path]
        if len(ht['files_folders']) > (len(data) + offset):
            ht['files_folders'] = ht['files_folders'][:offset] + data + ht['files_folders'][offset:]
        else:
            ht['files_folders'] = ht['files_folders'][:offset] + data
        ht['st_size'] = len(ht['files_folders'])
       
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
    del mht[1]
    #db.put(Binary('hello'), Binary('random'), 20000)
    #print db.get(Binary('hello'))
    #Modified by Anirudh Subramanian to enable logging Begin
    fuse = FUSE(Memory(mht), argv[1], foreground=True, debug=True)
    #Modified by Anirudh Subramanian to enable logging End
