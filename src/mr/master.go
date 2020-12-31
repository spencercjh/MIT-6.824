package mr

import (
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	SchedulerInterval = time.Millisecond * 100
	TaskTimeout       = time.Second * 10
)

type Master struct {
	// Your definitions here.
	// pg-*.txt filenames
	files []string
	// here it equals files amount
	mapTaskAmount int
	// given by mrmaster.go
	reduceTaskAmount int
	// map or reduce phase
	taskPhase TaskPhase
	// whether all tasks finished
	done bool

	// update following shared fields needs to be lock
	mutex sync.Mutex
	// task queue
	tasks chan Task
	// each task's status
	taskStatuses []TaskStatus
	// worker sequence
	workerSequence int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.done
}

func (m *Master) initMapTasks() {
	m.taskPhase = MAP_PHASE
	m.taskStatuses = make([]TaskStatus, m.mapTaskAmount)
	LogDebug("Master init %d map tasks", m.mapTaskAmount)
}

func (m *Master) tickScheduler() {
	for !m.Done() {
		go m.scheduler()
		time.Sleep(SchedulerInterval)
	}
}

func (m *Master) scheduler() {
	//LogDebug("Scheduler triggered")

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.Done() {
		return
	}

	allFinished := true
	for index, taskStatus := range m.taskStatuses {
		switch taskStatus.Status {
		// add task to the channel
		case READY:
			allFinished = false
			m.tasks <- m.setupOneTask(index)
			m.taskStatuses[index].Status = IN_QUEUE
		case IN_QUEUE:
			allFinished = false
		case RUNNING:
			allFinished = false
			// if the task is timeout, change its status and add it to channel again
			if time.Now().Sub(taskStatus.StartTime) > TaskTimeout {
				m.taskStatuses[index].Status = IN_QUEUE
				m.tasks <- m.setupOneTask(index)
			}
		case FINISHED:
		case ERROR:
			// if there is an error about the task, change its status and add it to channel again
			allFinished = false
			m.taskStatuses[index].Status = IN_QUEUE
			m.tasks <- m.setupOneTask(index)
		default:
			panic("Illegal TaskStatus")
		}
	}
	if allFinished {
		LogDebug("All %s tasks finished", m.taskStatuses)
		if m.taskPhase == MAP_PHASE {
			m.initReduceTasks()
		} else {
			m.done = true
		}
	}
}

func (m *Master) setupOneTask(index int) Task {
	task := Task{
		TaskId:        index,
		TaskPhase:     m.taskPhase,
		Filename:      "",
		NumMapTask:    m.mapTaskAmount,
		NumReduceTask: m.reduceTaskAmount,
		Alive:         true,
	}
	if task.TaskPhase == MAP_PHASE {
		task.Filename = m.files[index]
	}
	LogDebug("setup one task: %v", task)
	return task
}

//goland:noinspection GoUnusedParameter
func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	LogDebug("Receive worker request RegisterWorker")

	m.mutex.Lock()
	defer m.mutex.Unlock()

	reply.WorkerId = m.workerSequence
	m.workerSequence++
	LogDebug("Send worker id: %d to worker", reply.WorkerId)
	return nil
}

func (m *Master) RegisterTask(args *RegisterTaskArgs, reply *RegisterTaskReply) error {
	LogDebug("Receive worker request RegisterTask")

	task := <-m.tasks
	if task.Alive {
		if task.TaskPhase != m.taskPhase {
			panic("Illegal request task phase")
		}

		m.mutex.Lock()
		defer m.mutex.Unlock()

		m.taskStatuses[task.TaskId].Status = RUNNING
		m.taskStatuses[task.TaskId].WorkerId = args.WorkerId
		m.taskStatuses[task.TaskId].StartTime = time.Now()
	}
	reply.Task = task
	LogDebug("Send task: %v to worker", reply.Task)
	return nil
}

func (m *Master) initReduceTasks() {
	m.taskPhase = REDUCE_PHASE
	m.taskStatuses = make([]TaskStatus, m.reduceTaskAmount)
	LogDebug("Master init %d reduce tasks", m.reduceTaskAmount)
}

func (m *Master) ReportTaskStatus(args *ReportTaskStatusArgs, reply *ReportTaskStatusReply) error {
	LogDebug("Receive worker request ReportTaskStatus with args: %v", args)
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.taskPhase != args.TaskPhase || args.WorkerId != m.taskStatuses[args.TaskId].WorkerId {
		reply.Error = errors.New("illegal task")
		return nil
	}

	if args.Done {
		m.taskStatuses[args.TaskId].Status = FINISHED
	} else {
		m.taskStatuses[args.TaskId].Status = ERROR
	}

	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.workerSequence = 1
	m.reduceTaskAmount = nReduce
	m.mapTaskAmount = len(files)
	m.files = files
	m.mutex = sync.Mutex{}

	var channelLength int
	if m.mapTaskAmount > m.reduceTaskAmount {
		channelLength = m.mapTaskAmount
	} else {
		channelLength = m.reduceTaskAmount
	}
	m.tasks = make(chan Task, channelLength)

	m.initMapTasks()

	go m.tickScheduler()
	m.server()
	LogDebug("Master server started")
	return &m
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	_ = rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	_ = os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
