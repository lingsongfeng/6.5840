package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(mapf func(string, string) []KeyValue, response HeartbeatResponse) {
	file, err := os.Open(response.FileName)
	if err != nil {
		log.Printf("[worker] open file failed: \"%s\"", response.FileName)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Println("read file failed: ", response.FileName)
		return
	}
	file.Close()

	// 生成 <word0, 1> <word1, 1> <word2, 1> ...
	kva := mapf(response.FileName, string(content))

	// 将kva打到 nreduces 个文件中
	// reduces[x]表示第x个reduce文件存储的kv对数组
	reduces := make([][]KeyValue, response.NReduces)
	for _, kv := range kva {
		idx := ihash(kv.Key) % response.NReduces
		reduces[idx] = append(reduces[idx], kv)
	}

	uuid := rand.Int()

	for reduceId, l := range reduces {
		fileName := IntermediateFileName(response.Id, reduceId) + fmt.Sprintf(".%d", uuid)
		file, err := os.Create(fileName)
		if err != nil {
			log.Println("create file failed: ", fileName)
			return
		}
		enc := json.NewEncoder(file)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				err := enc.Encode(&kv)
				if err != nil {
					// TODO(lingsong.feng)
					return
				}
			}
		}
		if err := file.Close(); err != nil {
			// TODO(lingsong.feng)
		}
	}

	//log.Printf("[worker] done mapping file=%s\n", response.FileName)

	doReportTask(MapJob, response.Id, uuid)
}

func doReduceTask(reducef func(string, []string) string, response HeartbeatResponse) {

	//log.Printf("[worker] starting reduce task %d\n", response.Id)

	mp := make(map[string][]string)

	uuid := rand.Int()

	for mapId := 0; mapId < response.NMaps; mapId++ {
		fileName := IntermediateFileName(mapId, response.Id)
		if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
			continue
		}
		file, err := os.Open(fileName)
		defer file.Close()
		if err != nil {
			fmt.Printf("[worker] open file failed: \"%s\"", fileName)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			mp[kv.Key] = append(mp[kv.Key], kv.Value)
		}
	}

	{
		filename := OutFileName(response.Id)
		ofile, err := os.Create(filename + fmt.Sprintf(".%d", uuid))
		defer ofile.Close()
		if err != nil {
			log.Printf("error open file %v\n", filename)
			return
		}

		for k, v := range mp {
			output := reducef(k, v)
			fmt.Fprintf(ofile, "%v %v\n", k, output)
		}
	}

	//log.Printf("[worker] done reduce task %d\n", response.Id)

	doReportTask(ReduceJob, response.Id, uuid)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	rand.Seed(time.Now().UnixNano())

	for {
		response := doHeartbeat()
		switch response.JobType {
		case MapJob:
			doMapTask(mapf, response)
		case ReduceJob:
			doReduceTask(reducef, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case ShutdownJob:
			goto Exit
		default:
			log.Printf("[worker] unexpected JobType %v\n", response.JobType)
			goto Exit
			//panic(fmt.Sprintf("unexpected JobType %v", response.JobType))
		}
	}
Exit:
}

func doHeartbeat() HeartbeatResponse {
	request := HeartbeatRequest{}
	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &request, &response)
	return response
}

func doReportTask(jobType JobType, id int, uuid int) {
	request := ReportRequest{JobType: jobType, Id: id, Uuid: uuid}
	response := ReportResponse{}
	ok := call("Coordinator.Report", &request, &response)
	if !ok {
		panic("")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
