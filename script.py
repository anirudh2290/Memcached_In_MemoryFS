import memcache
import time
import sys
import getopt

print 'Hello'

#mc = memcache.Client(['127.0.0.1:11212'], debug=0)
"""
def fileWr(size):
	file = open("fusemount/test2.txt","w")
	size = int(size)
	
	str1 = 'a'*size*1024

	file.write(str(str1))

	file.close()	
"""

def fileWr(size):
	str1 = ""
	for i  in range(0, 1000):
		str1 = str1 + str(i)
		file = open("fusemount/trest" + str1 +".txt", "w+")
		size = int(size)		
		str1 = 'a'*size*1024
		file.write(str(str1))
		str1 = ''
		file.close()	
		 

def main(argv):
	try:
		opts, args = getopt.getopt(argv, "w:", "w=")
	except getopt.GetoptError:
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-w':
			w = arg
			if w == '':
				fileWr(4)
			else:
				fileWr(w)

if __name__ == "__main__":
	main(sys.argv[1:])

	


