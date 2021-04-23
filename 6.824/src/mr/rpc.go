package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// errcode of TaskResponse
const (
        ErrSuccess = iota
        ErrWait 
        ErrAllDone
)
// type of task.TaskType
const (
        TypeMap = iota + 1
        TypeReduce
)
// status of task
const (
        StatusReady = iota + 1
        StatusSent 
        StatusFinish
)
type Task struct {
        TaskId   int32
        TaskType int32
        Content  string // map task =>file_name, reduce task => reduce number
        Status   int32
}
type TaskRequest struct {

}
type TaskResponse struct {
        ErrCode int32
        Task 	Task
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
