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
	index	  int32
	mu	  sync.Mutex // the coordinator, as an RPC server, will be concurrent; don't forget to lock shared data. 
	status	  int32
	nReduce	  int32
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(req *TaskRequest, resp *TaskResponse) error {
	// do

	return nil
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
