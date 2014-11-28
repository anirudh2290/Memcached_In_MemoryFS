import memcache
import time
import sys

print 'Hello'

mc = memcache.Client(['127.0.0.1:11212'], debug=0)

mc.set("some_key", "Some value", 1)

i = 1
value = mc.get("some_key")
current_time = time.time() 
limit = sys.argv[1]
print limit
while(True):
	value = mc.get("some_key")
	if value:
		j = 3
	else:
		print "i = " + str(i) + "is set for : " + str(time.time() - current_time)
		current_time = time.time()
		i = i*2
		mc.set("some_key", "Some value", i)
