package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type GlobalPhase int

const (
	Uninitialized GlobalPhase = 0
	MappingPhase  GlobalPhase = 1
	ReducingPhase GlobalPhase = 2
	ShutingDown   GlobalPhase = 3
	Finished      GlobalPhase = 4
)

type TaskStatus int

const (
	Unstarted TaskStatus = 0
	Running   TaskStatus = 1
	Done      TaskStatus = 2
)

type MapTask struct {
	fileName string
	expireAt time.Time
	status   TaskStatus
}

type ReduceTask struct {
	expireAt time.Time
	status   TaskStatus
}

func CalculateMapTaskExpireAt() time.Time {
	return time.Now().Add(10 * time.Second)
}

func CalculateReduceTaskExpireAt() time.Time {
	return time.Now().Add(10 * time.Second)
}

type Coordinator struct {
	// Your definitions here.
	files    []string
	nMaps    int
	nReduces int

	phase GlobalPhase

	mapTasks    []MapTask    // id 就是下标
	reduceTasks []ReduceTask // id 就是下标

	watchDogTicker time.Ticker

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case Uninitialized:
		response.JobType = WaitJob
	case MappingPhase:
		found := false
		// 找到第一个未分配的mapping task
		for i, _ := range c.mapTasks {
			if c.mapTasks[i].status == Unstarted {
				c.mapTasks[i].status = Running
				c.mapTasks[i].expireAt = CalculateMapTaskExpireAt()
				found = true
				response.JobType = MapJob
				response.FileName = c.mapTasks[i].fileName
				response.Id = i
				response.NReduces = c.nReduces
				break
			}
		}
		if !found {
			response.JobType = WaitJob
		}
	case ReducingPhase:
		found := false
		for i, _ := range c.reduceTasks {
			if c.reduceTasks[i].status == Unstarted {
				c.reduceTasks[i].status = Running
				c.reduceTasks[i].expireAt = CalculateReduceTaskExpireAt()

				found = true
				response.JobType = ReduceJob
				response.NMaps = c.nMaps
				response.Id = i
				break
			}
		}
		if !found {
			response.JobType = WaitJob
		}
	case ShutingDown:
		response.JobType = ShutdownJob
	}

	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	uuid_s := fmt.Sprint(request.Uuid)
	// commit task
	filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
			//panic(fmt.Sprintf("%v %v", err, path))
		}
		if strings.HasSuffix(path, uuid_s) {
			newName := strings.TrimSuffix(path, filepath.Ext(path))
			if _, err := os.Stat(newName); os.IsNotExist(err) {
				os.Rename(path, newName)
			}
		}
		return nil
	})

	switch request.JobType {
	case MapJob:
		c.mapTasks[request.Id].status = Done
		allDone := true
		for _, task := range c.mapTasks {
			if task.status != Done {
				allDone = false
				break
			}
		}
		if allDone && c.phase == MappingPhase {
			c.phase = ReducingPhase
		}
	case ReduceJob:
		c.reduceTasks[request.Id].status = Done
		allDone := true
		for _, task := range c.reduceTasks {
			if task.status != Done {
				allDone = false
				break
			}
		}
		if allDone && c.phase == ReducingPhase {
			c.phase = ShutingDown
		}
	default:
		log.Printf("cannot resolve report type %v", request.JobType)
	}

	return nil
}

func (c *Coordinator) setFinished() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.phase = Finished
	c.watchDogTicker.Stop()
}

func (c *Coordinator) checkAndRestartExpiredTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	mapTaskStatus := []TaskStatus{}
	for _, task := range c.mapTasks {
		mapTaskStatus = append(mapTaskStatus, task.status)
	}
	reduceTaskStatus := []TaskStatus{}
	for _, task := range c.reduceTasks {
		reduceTaskStatus = append(reduceTaskStatus, task.status)
	}
	//log.Printf("[watchdog] status: map=%v reduce=%v\n", mapTaskStatus, reduceTaskStatus)

	now := time.Now()
	for i, _ := range c.mapTasks {
		if c.mapTasks[i].status == Running && now.After(c.mapTasks[i].expireAt) {
			c.mapTasks[i].status = Unstarted
			log.Printf("[watchdog] restarting map task %d\n", i)
		}
	}
	for i, _ := range c.reduceTasks {
		if c.reduceTasks[i].status == Running && now.After(c.reduceTasks[i].expireAt) {
			c.reduceTasks[i].status = Unstarted
			log.Printf("[watchdog] restarting reduce task %d\n", i)
		}
	}
	if c.phase == ShutingDown {
		timer := time.NewTimer(2 * time.Second)
		go func() {
			<-timer.C
			// after 2 seconds, all workers should have been shut down elegantly
			c.setFinished()
		}()
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := c.phase == Finished

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	mapTasks := []MapTask{}
	// TODO: use range based for
	for i := 0; i < len(files); i++ {
		mapTasks = append(mapTasks, MapTask{fileName: files[i], status: Unstarted})
	}

	reduceTasks := []ReduceTask{}
	for i := 0; i < nReduce; i++ {
		reduceTasks = append(reduceTasks, ReduceTask{status: Unstarted})
	}

	c := Coordinator{
		files:          files,
		nMaps:          len(files),
		nReduces:       nReduce,
		phase:          Uninitialized,
		mapTasks:       mapTasks,
		reduceTasks:    reduceTasks,
		watchDogTicker: *time.NewTicker(1 * time.Second),
		mu:             sync.Mutex{},
	}

	c.phase = MappingPhase

	go func() {
		for {
			<-c.watchDogTicker.C
			if c.Done() {
				c.watchDogTicker.Stop()
				break
			}
			c.checkAndRestartExpiredTasks()
		}
	}()

	log.Printf("[coordinator] nMaps=%d nReduces=%d\n", c.nMaps, c.nReduces)

	c.server()

	return &c
}
