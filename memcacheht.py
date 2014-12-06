# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=3

import xmlrpclib, pickle, memcache
from xmlrpclib import Binary
"""
   memcacheht class to be used by the filesystem to put and get objects into the filesystem

"""


class memcacheht:
   CONSTANT_TIMEOUT = 20000
   def __init__(self, serverList):
      """
         Use the serverList to connect with the client. If no serverList given then use the 
         default local memcached server.   
      """
      print "Initialize serverList"
      print serverList 
      print "Initialize serverList"

      if (len(serverList) <= 0):
         self.mc = memcache.Client(['127.0.0.1:11211'], debug=0)
      else:
	 print "Inside else"
         self.mc = memcache.Client(serverList, debug=0)
   
   def __setitem__(self, key, value):
      print 'Inside put'
      print 'key is ' + str(key)
      print 'value is ' + str(value)
      self.mc.set(str(key), value, memcacheht.CONSTANT_TIMEOUT)
  
   
   def __getitem__(self, key):
      val = self.mc.get(str(key))
      if val == None:
         raise KeyError()
      return val
   
   def __delitem__(self, key):
      #self.mc.set(str(key), "", 0) 
      self.mc.delete(str(key))
   
   def get(self, key):
      return self.mc.get(str(key))

   def __contains__(self, key):
      return self.get(key) != None   
         
def main():
   #serverList = ['127.0.0.1:11211', '127.0.0.1:11212']
   #serverList = ['127.0.0.1:11211']
   ht2 = memcacheht(serverList)
   #sList = []
   #ht3 = memcacheht(sList)
   #ht2.put("mm1'", dict(one=1, two=2), 20000)
   #tp = ht2.get("mm1'")
   #print tp.get("two")
   ht2['mm1'] = dict(one=1, two=2)
   print ht2['mm1']['two']
   #print ht2['mm2']

if __name__ == '__main__':
   main()
