package mr

import (
	"fmt"
	"io"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// 第一次启动的时候先发送rpc到master那里获取workerID
func getWorkerID() (int, error) {
	args := GetWorkerIDArgs{}
	reply := GetWorkerIDReply{}
	ok := call("Coordinator.GetWorkerID", &args, &reply)
	if ok {
		return reply.WorkerID, nil
	} else {
		return 0, fmt.Errorf("failed to call Coordinator.GetWorkerID")
	}
}

// 请求任务
func requestTask() (*RequestTaskReply, error) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return &reply, nil
	} else {

		return nil, fmt.Errorf("failed to call Coordinator.AssignTask")
	}
}

// 处理map任务
func doMap(mapf func(string, string) []KeyValue, reply *RequestTaskReply) (*RequestTaskReply, error) {
	//args := RequestTaskArgs{WorkerID: workerID}
	//reply := RequestTaskReply{}

	//ok := call("Coordinator.AssignTask", &args, &reply)

	intermediate := []KeyValue{}
	filename := reply.FileName
	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	writeIntermediateFiles(reply.TaskID, intermediate, reply.NReduce)

	const maxRetries = 3
	const retryInterval = time.Second

	reportTaskArgs := ReportTaskArgs{
		TaskType: reply.TaskType,
		TaskID:   reply.TaskID,
	}
	reportTaskReply := ReportTaskReply{}
	success := false
	for i := 0; i < maxRetries; i++ {
		ok := call("Coordinator.ReportTaskCompletion", &reportTaskArgs, &reportTaskReply)
		if ok {
			success = true
			break
		}
		time.Sleep(retryInterval)
	}
	if !success {
		// 处理失败情况，例如记录日志或终止程序
		log.Printf("RPC 调用失败: Coordinator.ReportTaskCompletion，任务类型: %v，任务ID: %d", reply.TaskType, reply.TaskID)
	}

	return reply, nil
}

// 处理reduce任kv a
func doReduce(reducef func(string, []string) string, reply *RequestTaskReply) (*RequestTaskReply, error) {
	reduceTaskNumber := reply.TaskID
	nMap := reply.NMap
	//fmt.Printf("reduceTaskNumber: %v\n", reduceTaskNumber)
	//fmt.Printf("nMap: %v\n", nMap)
	intermediate, err := readIntermediateFiles(reduceTaskNumber, nMap)
	if err != nil {
		log.Fatal("readIntermediateFiles error:", err)
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceTaskNumber)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// test
	const maxRetries = 3
	const retryInterval = time.Second

	reportTaskArgs := ReportTaskArgs{
		TaskType: reply.TaskType,
		TaskID:   reply.TaskID,
	}
	reportTaskReply := ReportTaskReply{}
	success := false
	for i := 0; i < maxRetries; i++ {
		ok := call("Coordinator.ReportTaskCompletion", &reportTaskArgs, &reportTaskReply)
		if ok {
			success = true
			break
		}
		time.Sleep(retryInterval)
	}
	if !success {
		// 处理失败情况，例如记录日志或终止程序
		log.Printf("RPC 调用失败: Coordinator.ReportTaskCompletion，任务类型: %v，任务ID: %d", reply.TaskType, reply.TaskID)
	}

	return reply, nil
	//	test
}

func writeIntermediateFiles(mapTaskNumber int, kva []KeyValue, nReduce int) error {
	log.Printf("nreduce是 %+v\n", nReduce)
	encoders := make([]*json.Encoder, nReduce)
	files := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskNumber, i)
		file, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("cannot create file %v: %v", filename, err)
		}
		files[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % nReduce
		err := encoders[reduceTaskNumber].Encode(&kv)
		if err != nil {
			return fmt.Errorf("cannot encode kv pair %v: %v", kv, err)
		}
	}
	for _, file := range files {
		file.Close()
	}
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	//先获取workerID
	workerID, _ := getWorkerID()
	fmt.Println(workerID)
	for {
		reply, err := requestTask()
		if err != nil {
			log.Fatalf("Failed to request task: %v", err)
		}
		switch reply.TaskType {
		case TaskTypeMap:
			doMap(mapf, reply)
		case TaskTypeWait:
			time.Sleep(1 * time.Second)
		case TaskTypeReduce:
			doReduce(reducef, reply)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

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

//func (m *Master) ReportTaskCompletion(args *ReportTaskArgs, reply *ReportTaskReply) error {
//	m.mu.Lock()
//	defer m.mu.Unlock()
//
//	if args.TaskType == TaskTypeMap {
//		if args.TaskID >= 0 && args.TaskID < len(m.tasks) {
//			m.tasks[args.TaskID].Status = Completed
//		}
//	}
//
//	// 处理 Reduce 任务的完成情况...
//
//	return nil
//}

func readIntermediateFiles(reduceTaskNumber int, nMap int) ([]KeyValue, error) {
	var kva []KeyValue
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceTaskNumber)
		fmt.Println(filename)
		file, err := os.Open(filename)
		if err != nil {
			return nil, fmt.Errorf("cannot open file %v: %v", filename, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				return nil, fmt.Errorf("cannot decode kv pair from file %v: %v", filename, err)
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva, nil
}
