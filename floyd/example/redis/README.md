### Raftis

raftis is a consensus server support redis protocol(get/set command). raftis is an example of building a consensus cluster with floyd(floyd is a simple implementation of raft protocol). It's very simple and intuitive. we can test raftis with redis-cli, benchmark with redis redis-benchmark tools, here is the result test with redis-benchmark

```
└─[$] ./src/redis-benchmark -t set -n 1000000 -r 100000000 -c 20
====== SET ======
  1000000 requests completed in 219.76 seconds
  20 parallel clients
  3 bytes payload
  keep alive: 1

0.00% <= 2 milliseconds
0.00% <= 3 milliseconds
8.72% <= 4 milliseconds
95.39% <= 5 milliseconds
95.96% <= 6 milliseconds
99.21% <= 7 milliseconds
99.66% <= 8 milliseconds
99.97% <= 9 milliseconds
99.97% <= 11 milliseconds
99.97% <= 12 milliseconds
99.97% <= 14 milliseconds
99.97% <= 15 milliseconds
99.99% <= 16 milliseconds
99.99% <= 17 milliseconds
99.99% <= 18 milliseconds
99.99% <= 19 milliseconds
99.99% <= 26 milliseconds
99.99% <= 27 milliseconds
100.00% <= 28 milliseconds
100.00% <= 29 milliseconds
100.00% <= 30 milliseconds
100.00% <= 61 milliseconds
100.00% <= 62 milliseconds
100.00% <= 63 milliseconds
100.00% <= 63 milliseconds
4550.31 requests per second
```
