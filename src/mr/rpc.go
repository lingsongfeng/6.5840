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

type JobType int

const (
	Undef       JobType = 0
	MapJob      JobType = 1
	ReduceJob   JobType = 2
	WaitJob     JobType = 3
	ShutdownJob JobType = 4
)

type HeartbeatRequest struct {
}
type HeartbeatResponse struct {
	JobType  JobType
	Id       int    // map任务id 或 reduce任务编号，由JobType决定
	FileName string // 只有在map任务时有意义
	NMaps    int    // 只有在reduce任务时有意义
	NReduces int    // 只有在map任务时有意义
}

type ReportRequest struct {
	JobType JobType
	Id      int // map任务id 或 reduce任务编号，由JobType决定
	Uuid    int
}

type ReportResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
