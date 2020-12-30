package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

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
// 2. worker register(get) a task from Master by the workerId
// 3. do the task
// 3.1 MAP_PHASE task
// 3.2 REDUCE_PHASE task
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

func (w worker) doTask(task Task) Task {
	LogDebug("Worker begin doing task")
	switch task.TaskPhase {
	case MAP_PHASE:
		return w.doMapTask(task)
	case REDUCE_PHASE:
		return w.doReduceTask(task)
	default:
		panic("Wrong phase")
	}
}

func (w worker) doMapTask(task Task) Task {
	return task
}

func (w worker) doReduceTask(task Task) Task {
	return task
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
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
