package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

// status of Coordinator
const (
	MapPeroid = iota + 1
	ReducePeroid
	AllDone
)
type Coordinator struct {
	// Your definitions here.
	taskQueue []*Task
	index	  int32	     // curent free task
	mu	  sync.Mutex // the coordinator, as an RPC server, will be concurrent; don't forget to lock shared data. 
	status	  int32
	nReduce	  int32
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(req *TaskRequest, resp *TaskResponse) error {
	// maybe there are many workers(you can see a worker as a goroutine) connect to coordinator ask for tasks,
	// so need to add lock.	
	// Lock() locks c. If the lock is already in use, the calling goroutine(worker) blocks until the mutex is available.
	c.mu.Lock() 
	defer.c.mu.Unlock()
	
	// traverse c.taskQueue, and ask for a task	
	hasWaiting := false
	n := len(c.taskQueue)
	for i := 0; i < n; i++ {
		t := c.taskQueue[c.index]
		c.index = (c.index + 1) % n
		
		// ask for task successfully
		if t.Status == StatusReady {
			t.Status == StatusSent	
			resp.Task = *t
			
			resp.ErrCode = ErrSuccess

			return nil
		} else if t.Status == StatusSent {
			hasWaitring = true	
		}
	}

	// traversal the whole c.taskQueue, there are still unfinished task,
	// tell worker just wait the unfinished task to finish.
	if hasWaiting {
		resp.ErrCode = ErrWait
		return nil
	}

	// finish all map tasks during mapperoid or reduce tasks during reduceperoid
	switch c.status {
	case MapPeroid:
		// all map tasks done, transform to reduce step
		c.status = ReducePeroid
		loadReduceTask(c)
		resp.Errcode = ErrWait
		return nil
	case ReducePeroid:
		// end coordinator	
		c.status = AllDone			
		// end worker	
		resp.ErrCode = ErrAllCode
		return nil
	}
	
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// load all map tasks to c.taskQueue 
func loadMapTask(c *Coordinator,filenames []string) {
	c.taskQueue = make([]*Task, 0)
	c.index = 0	
	n := len(filenames)	
	for i := 0; i < n; i++ {
		c.taskQueue = append(c.taskQueue, &Task{
			TaskId:    int32(i)
			TaskType:  TypeMap
			Content:   filenames[i]
			Status:	   StatusReady
		})	
	}
}
//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true 

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		status: MapPeroid
		nReduce: int32(nReduce)
	}

	// Your code here.
	loadMapTasks(&c, files)

	c.server()
	return &c
}
