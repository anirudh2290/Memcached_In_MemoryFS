# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=3

import xmlrpclib, pickle, memcache
from xmlrpclib import Binary
"""
   memcacheht class to be used by the filesystem to put and get objects into the filesystem

"""


class memcacheht:
   CONSTANT_TIMEOUT = 2000
   def __init__(self, serverList):
      """
         Use the serverList to connect with the client. If no serverList given then use the 
         default local memcached server.   
      """
      
      if (len(serverList) <= 0):
         self.mc = memcache.Client(['127.0.0.1:11211'], debug=0)
      else: 
         self.mc = memcache.Client(serverList, debug=0)
   
   def put(self, key, value, ttl):
      print 'Inside put'
      print 'key is ' + str(key)
      print 'value is ' + str(value)
      self.mc.set(str(key), value, ttl)
   
   def get(self, key):
      return self.mc.get(str(key))
      
         
def main():
   serverList = ['127.0.0.1:11211', '127.0.0.1:11212']
   ht2 = memcacheht(serverList)
   #sList = []
   #ht3 = memcacheht(sList)
   ht2.put("mm1'", dict(one=1, two=2), 20000)
   tp = ht2.get("mm1'")
   print tp.get("two")


if __name__ == '__main__':
   main()
