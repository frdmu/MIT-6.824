## Website
- https://pdos.csail.mit.edu/6.824/schedule.html

## Refer
- https://github.com/JellyZhang/mit-6.824-2021
- https://github.com/Wangzhike/MIT6.824_DistributedSystem

## Lab1: MapReduce
In the main directory, run the coordinator. 
```
$ go build -race -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run -race mrcoordinator.go pg-*.txt
```
In one or more other windows, run some workers: 
```
$ go run -race mrworker.go wc.so
```
When the workers and coordinator have finished, look at the output in mr-out-*. When you've completed the lab, the sorted union of the output files should match the sequential output, like this: 
```
$ cat mr-out-* | sort | more
A 509
ABOUT 2
ACT 8
...
```
We supply you with a test script in main/test-mr.sh. The tests check that the wc and indexer MapReduce applications produce the correct output when given the pg-xxx.txt files as input. The tests also check that your implementation runs the Map and Reduce tasks in parallel, and that your implementation recovers from workers that crash while running tasks. 
```
$ bash test-mr.sh
```


