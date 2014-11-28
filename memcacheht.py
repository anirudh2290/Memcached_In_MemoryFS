# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=3

import xmlrpclib, pickle, memcache
#from xmlrpclib import Binary
"""
   memcacheht class to be used by the filesystem to put and get objects into the filesystem

"""


class memcacheht:
   def __init__(self, serverList):
      """
         Use the serverList to connect with the client. If no serverList given then use the 
         default local memcached server.   
      """
      if (len(serverList) <= 0):
         mc = memcache.Client(['127.0.0.1:11213'], debug=0)
      else: 
         mc = memcache.Client(serverList, debug=0)

      mc.set("some_key", "some val")
      print mc.get("some_key")


def main():
   serverList = ['172.31.26.176:11211']
   ht2 = memcacheht(serverList)
   sList = []
   ht3 = memcacheht(sList)

if __name__ == '__main__':
   main()
