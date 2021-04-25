package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
import "time"
const WorkerDieTime = 10 * time.Second
// status of Coordinator
const (
	MapPeroid = iota + 1
	ReducePeroid
	AllDone
)
type Coordinator struct {
	// Your definitions here.
	taskQueue []*Task
	index	  int	     // curent free task
	mu	  sync.Mutex // the coordinator, as an RPC server, will be concurrent; don't forget to lock shared data. 
	status	  int32
	nReduce	  int32
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(req *TaskRequest, resp *TaskResponse) error {
	c.mu.Lock() 
	defer c.mu.Unlock()

	// traverse c.taskQueue, and find a task	
	hasWaiting := false
	n := len(c.taskQueue)
	for i := 0; i < n; i++ {
		t := c.taskQueue[c.index]
		c.index = (c.index + 1) % n
		
		// ask for task successfully
		if t.Status == StatusReady {
			t.Status = StatusSent	
			resp.Task = *t
			resp.ErrCode = ErrSuccess
			go checkTask(c, t.TaskId, t.TaskType)	
			return nil
		} else if t.Status == StatusSent {
			// some tasks has not finished
			hasWaiting = true	
		}
	}

	// traversal the whole c.taskQueue, there are still unfinished task,
	// tell worker just wait the unfinished task to finish.
	if hasWaiting {
		resp.ErrCode = ErrWait
		return nil
	}

	// finish all map tasks during mapperoid 
	// or reduce tasks during reduceperoid
	switch c.status {
	case MapPeroid:
		c.status = ReducePeroid
		loadReduceTasks(c)
		resp.ErrCode = ErrWait
		return nil
	case ReducePeroid:
		// end coordinator	
		c.status = AllDone			
		// end worker	
		resp.ErrCode = ErrAllDone
		return nil
	}
	return nil
}

// check one task is finished or not after it given out 10 seconds.
// if Not finished, meaning that worker might creshed. Reset the task.Status to Ready, so that it can be given out again.
func checkTask(c *Coordinator, taskId int32, taskType int32) {
	time.Sleep(WorkerDieTime)
	//log.Printf("[checkTask] taskId=%v, taskType=%v", taskId, taskType)
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.taskQueue[taskId].TaskType != taskType {
		//log.Println("Too old taskType, ignore")
		// this means we are already at reduce-period, and this func try to check one of the map-tasks.
		// since we can get into reduce-period, which means map-tasks all done.
		// so just ignore it.
		return
	}
	if c.taskQueue[taskId].Status == StatusSent {
		//log.Printf("put taskId=%v back", taskId)
		c.taskQueue[taskId].Status = StatusReady
	}
}

// Notify by worker about which task is done
func (c *Coordinator) Notify(req *NotifyRequest, resp *NotifyResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.taskQueue[req.TaskId].Status = StatusFinish		
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
func loadMapTasks(c *Coordinator,filenames []string) {
	c.taskQueue = make([]*Task, 0)
	c.index = 0	
	n := len(filenames)	
	for i := 0; i < n; i++ {
		c.taskQueue = append(c.taskQueue, &Task{
			TaskId:    int32(i),
			TaskType:  TypeMap,
			Content:   filenames[i],
			Status:	   StatusReady,
		})	
	}
}

// load all reduce tasks to c.taskQueue 
func loadReduceTasks(c *Coordinator) {
	c.taskQueue = make([]*Task, 0)
	c.index = 0	
	for i := 0; int32(i) < c.nReduce; i++ {
		c.taskQueue = append(c.taskQueue, &Task{
			TaskId:    int32(i),
			TaskType:  TypeReduce,
			Content:   fmt.Sprint(c.nReduce),
			Status:	   StatusReady,
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
	ret := false 

	// Your code here.
	if c.status == AllDone {
		ret = true	
	}		

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		status: MapPeroid,
		nReduce: int32(nReduce),
	}

	// Your code here.
	loadMapTasks(&c, files)

	c.server()
	return &c
}
