package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType int

const (
	TaskTypeNone TaskType = iota
	TaskTypeMap
	TaskTypeReduce
	TaskTypeWait
	TaskTypeExit
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetWorkerIDArgs struct {
}

type GetWorkerIDReply struct {
	WorkerID int
}

//type TaskType int

type RequestTaskArgs struct {
	WorkerID int
}
type RequestTaskReply struct {
	TaskType TaskType
	TaskID   int
	NReduce  int
	NMap     int
	FileName string
}

type ReportTaskArgs struct {
	TaskType TaskType
	TaskID   int
}
type ReportTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
