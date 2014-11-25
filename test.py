import memcache

print 'Hello'

mc = memcache.Client(['127.0.0.1:11211'], debug=0)

mc.set("some_key", "Some value", 1)

i = 1
value = mc.get("some_key")
current_time = 
while(True):
	value = mc.get("some_key")
	if value:
		print 'i value is ' + str(i)
		print 'value is ' + value
	else:
		i = i*2
		mc.set("some_key", "Some value", i)

