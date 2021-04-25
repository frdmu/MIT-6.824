package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "encoding/json"
import "os"
import "io/ioutil"
import "sort"
import "regexp"
import "strconv"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Send an RPC(remote pecedure call) to the coordinator asking for a task.
	for {
		req := &TaskRequest{}	
		resp := &TaskResponse{}
		if call("Coordinator.GetTask", req, resp) == true {
			switch resp.ErrCode {
				case ErrWait:
					time.Sleep(1 * time.Second)
					continue
				case ErrAllDone:
					break
				case ErrSuccess:
					switch resp.Task.TaskType {
					case TypeMap:
						DoMap(resp.Task, mapf)
					case TypeReduce:
						DoReduce(resp.Task, reducef)
					}
			}	
		} else {
			break	
		}
	}
}

// store kv just like {"key", "1"} in "mr-map-*" file
func DoMap(task Task, mapf func(string, string) []KeyValue) {
	// 1.map 
	filename := task.Content	
	file, _ := os.Open(filename)
	content, _ := ioutil.ReadAll(file)
	file.Close()
	kva := mapf(filename, string(content))

	// 2.create a temp file, and rename it after all written finished	
	oname := fmt.Sprintf("mr-map-%v", task.TaskId)	
	tmpfile, _ := ioutil.TempFile(".", "temp-"+ oname)
	
	enc := json.NewEncoder(tmpfile)
  	for _, kv := range kva {
    		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("[DoMap]encode save json err=%v\n", err)	
		}
	}
	tmpfile.Close()
	os.Rename(tmpfile.Name(), oname)

	// 3.notify coordinator this task is done
	NotifyCoordinator(task.TaskId, task.TaskType)
}

// read all "mr-map-*" files 
// do reduce for keys that satisfy ihash(key)%reduce == taskId,
// and store result in "mr-out-taskId"
func DoReduce(task Task, reducef func(string, []string) string) {
	// 1.read all "mr-map-*" files
	// add keys that satisfy ihash(key)%reduce == taskId in intermediate space kva
	var kva []KeyValue	
	files, _ := ioutil.ReadDir(".")	
	for _, file := range files {
		matched, _ := regexp.Match(`^mr-map-*`, []byte(file.Name()))	
		if !matched {
			continue	
		}
		filename := file.Name()
		file, _ := os.Open(filename)
		
		nReduce, _ := strconv.Atoi(task.Content)	
		dec := json.NewDecoder(file)
		for {
		    var kv KeyValue
		    if err := dec.Decode(&kv); err != nil {
		    	break
		    }
		    if ihash(kv.Key)%nReduce == int(task.TaskId) {
		    	kva = append(kva, kv)
		    }
		}
	}

	// do reduce just like in main/mrsequential.go
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%v", task.TaskId)
	tmpfile, _ := ioutil.TempFile(".", "temp-" + oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	tmpfile.Close()	
	os.Rename(tmpfile.Name(), oname)
	NotifyCoordinator(task.TaskId, task.TaskType)
}
// send Notify to coordinator, to tell coordinator that this task is finished
func NotifyCoordinator(taskId int32, taskType int32) {
	req := &NotifyRequest{
		TaskId: taskId,
		TaskType: taskType,
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
