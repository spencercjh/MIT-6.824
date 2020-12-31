package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	workerId       int
	mapFunction    func(string, string) []KeyValue
	reduceFunction func(string, []string) string
}

// 1. worker register itself and get a workerId from Master
// 2. loop
// 2.1 worker register(get) a task from Master by the workerId
// 2.2 do the task
// 2.2.1 MAP_PHASE task or
// 2.2.2 REDUCE_PHASE task
// 2.3 if there is no alive task, exit
func (w worker) run() {
	w.registerWorker()
	// if reqTask conn fail, worker exit
	for {
		task := w.registerTask()
		LogDebug("Receive task: %v", task)
		if !task.Alive {
			LogDebug("Task received from master is not alive, exit")
			return
		}
		w.doTask(task)
	}
}

func (w worker) registerWorker() int {
	LogDebug("Worker registerWorker")
	reply := RegisterWorkerReply{}
	call("Master.RegisterWorker", &RegisterWorkerArgs{}, &reply)
	LogDebug("Worker call Master.RegisterWorker and the response is: %v", reply)
	w.workerId = reply.WorkerId
	return reply.WorkerId
}

func (w worker) registerTask() Task {
	LogDebug("Worker registerTask")
	args := RegisterTaskArgs{}
	args.WorkerId = w.workerId
	reply := RegisterTaskReply{}
	call("Master.RegisterTask", &args, &reply)
	LogDebug("Worker call Master.RegisterTask with args: %v and the response is: %v", args, reply)
	return reply.Task
}

func (w worker) doTask(task Task) {
	LogDebug("Worker begin doing task")
	switch task.TaskPhase {
	case MAP_PHASE:
		w.doMapTask(task)
	case REDUCE_PHASE:
		w.doReduceTask(task)
	default:
		panic("Wrong phase")
	}
}

func (w worker) doMapTask(task Task) {
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v because %v", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v because %v", filename, err)
	}
	if err = file.Close(); err != nil {
		log.Fatalf("cannot close %v because %v", filename, err)
	}
	// do map
	keyValues := w.mapFunction(filename, string(content))

	// split the intermediate data into NumReduceTask parts
	reduces := make([][]KeyValue, task.NumReduceTask)
	for _, keyValue := range keyValues {
		index := ihash(keyValue.Key) % task.NumReduceTask
		reduces[index] = append(reduces[index], keyValue)
	}

	// save them
	for idx, lines := range reduces {
		reduceFileName := reduceName(task.TaskId, idx)
		reduceFile, err := os.Create(reduceFileName)
		if err != nil {
			w.reportTaskStatus(task, false, err)
		}
		enc := json.NewEncoder(reduceFile)
		for _, kv := range lines {
			if err := enc.Encode(&kv); err != nil {
				w.reportTaskStatus(task, false, err)
			}

		}
		if err := reduceFile.Close(); err != nil {
			w.reportTaskStatus(task, false, err)
		}
	}
	w.reportTaskStatus(task, true, nil)
}

func (w worker) doReduceTask(task Task) {
	// its format is: abc: 1,1,1,1,1,1,1,1,1,1 ...
	wordNums := make(map[string][]string)
	for i := 0; i < task.NumMapTask; i++ {
		reduceFileName := reduceName(i, task.TaskId)
		file, err := os.Open(reduceFileName)
		if err != nil {
			// report error
			panic(err)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			// key doesn't exist, put a pair <word,empty string array>
			if _, ok := wordNums[kv.Key]; !ok {
				wordNums[kv.Key] = make([]string, 0, 100)
			}
			wordNums[kv.Key] = append(wordNums[kv.Key], kv.Value)
		}
	}
	// store reduce result
	result := make([]string, 0, 1000)
	for word, nums := range wordNums {
		result = append(result, fmt.Sprintf("%v %v\n", word, w.reduceFunction(word, nums)))
	}

	// rwxrwxrwx and notice that perm is a uint32
	if err := ioutil.WriteFile(mergeName(task.TaskId), []byte(strings.Join(result, "")), 0777); err != nil {
		w.reportTaskStatus(task, false, err)
		return
	}
	w.reportTaskStatus(task, true, nil)
}

func (w *worker) reportTaskStatus(task Task, done bool, err error) {
	LogDebug("Worker reportTaskStatus")
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskStatusArgs{w.workerId, task.TaskId, done, task.TaskPhase}
	call("Master.ReportTaskStatus", &args, &ReportTaskStatusReply{})
	LogDebug("Worker call Master.ReportTaskStatus with args: %v", args)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	LogDebug("Worker begin")
	worker := worker{}
	worker.mapFunction = mapf
	worker.reduceFunction = reducef
	worker.run()
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)

	_ = c.Close()
	return false
}
