package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskStatus int

const (
	None     TaskStatus = 0
	Map      TaskStatus = 1
	Reduce   TaskStatus = 2
	Complete TaskStatus = 3
)

// No arguments to send the coordinator to ask for a task
type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskType     TaskStatus
	TaskNum      int
	NReduceTasks int
	NMapTasks    int
	MapFile      string
}

type FinishArgs struct {
	TaskType TaskStatus
	TaskNum  int
}

type FinishReply struct {
}

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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
