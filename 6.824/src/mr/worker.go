package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "encoding/json"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// Send an RPC to the coordinator asking for a task.
	// Then modify the worker to read that file and call the application Map function or the application Reduce function, as in mrsequential.go.
	for {
		req := &TaskRequest{}	
		resp := &TaskResponse{}
		// connect to coordinator, and call GetTask function to get a task	
		if call("Coordinator.GetTask", req, resp) == true {
			switch resp.ErrCode {
				case ErrWait:
					// workers will sometimes need to wait, 
					
					// e.g. reduces can't start until the last map has finished. 
                                        // one possibility is for workers to periodically ask the coordinator for work,
                                        // sleeping with time.Sleep() between each request.
					
					// another case: e.g. after all map tasks done, in GetTask we transform map step to reduce step,
					// but don't get a task.
					time.Sleep(time.Second)
					continue
				case ErrAllDone:
					// all job done, this worker can be closed	
					break
				case ErrSuccess:
					// do Map or reduce
					switch resp.Task.TaskType {
						case TypeMap:
							DoMap(resp.Task, mapf)
						case TypeReduce:
							DoReduce(resp.Task, reducef)
					}
			}	
		} else {
			// failed to contact the coordinator, it can assume that the coordinator has exited because the job is done.	
			break;	
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// store {"key", "1"} in "mr-map-*" file
func DoMap(task Task, mapf func(string, string) []KeyValue) {
	// 1.map 
	filename := task.Content	
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("[DoMap]cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[DoMap]cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// 2.create a temp file, and rename it after all written finished	
	oname := fmt.Sprintf("mr-map-%v", task.TaskId)	
	tmpFile, err := ioutil.TempFile(".", "temp-"+ oname)
	if err != nil {
		log.Fatal(err)	
	}	
	// to write key/value pairs to a JSON file
	enc := json.NewEncoder(tmpfile)
  	for _, kv := kva {
    		if err := enc.Encode(&kv) != nil {
			log.Fatalf("[DoMap]encode save json err=%v\n", err)	
		}
	}
	if err := tmpfile.Close() != nil {
		log.Fatal(err)	
	}
	os.Rename(tmpfile.Name(), oname)

	//3. notify coordinator that this task is done
	NotifyCoordinator(task.TaskId, task.TaskType)
}

func DoReduce(task Task, reducef func(string, []string) string) {

}
// send Notify to coordinator, to tell coordinator that this task is finished
func NotifyCoordinator(taskId int32, taskType int32) {
	req := &NotifyRequest{
		TaskId: taskId
		TaskType: taskType
	}
	resp := &NotifyResponse{}
	call("Coordinator.Notify", req, resp)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
