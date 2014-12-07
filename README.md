Memcached in memory filesystem

Instructions on how to mount the filesystem
-------------------------------------------


Start the memcached server
--------------------------

To run the memcached server run the following:

memcached -vvv -p 11212

Or if it is in a remote machine do:

memcached -vvv -l ip:port

Start the memcached client
--------------------------

To start the memcached client do the following :

python fuserpc _ edited.py fusemount/ 127.0.0.1:11211 127.0.0.1:11212 
(If the memcached servers are running on port 11211 and 11212 on the local machine)


To run the benchmarking tool
----------------------------

python script.py -w 1000

