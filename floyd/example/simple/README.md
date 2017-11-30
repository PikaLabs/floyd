### simple test cases

the simple example will run the Floyd:write the get performance of floyd in a simple machine

t is a single thread wirte tool to get performance

t1 is multi thread program to get performance, in this case, all the writes is from the leader node

t2 is an example test node join and leave

t4 is an example used to see the message passing by each node in a stable situation

t5 used to test single mode floyd, including starting a node and writing data

t6 is the same as t1 except that all the writes is from the follower node

t7 test write 3 node and then join the other 2 node case

t8 start three nodes, node1 has the shortest log, and it will start first. node1
will not be choosen as leader, node2/node3 will be choosen as leader, and the
leader's log will cover node1's log

test_lock test the base lock operation in floyd, the case is use lock and unlock in a simple thread

test_lock1 is the case that two thread preempt to a same lock

add_server test the base membership change proto, start 3 nodes and writing some data, then start the 4th node and join the group, at last start another node and join the group.

add_server1 is the case that join the group parallel with writing data

read_bench is an benchmark tool to get multi thread reading performance
