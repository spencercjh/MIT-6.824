package mr

import "time"

type TaskPhase int

const (
	MAP_PHASE    TaskPhase = 0
	REDUCE_PHASE TaskPhase = 1
)

type Status int

const (
	READY    Status = 0
	IN_QUEUE Status = 1
	RUNNING  Status = 2
	FINISHED Status = 3
	ERROR    Status = 4
)

type TaskStatus struct {
	Status    Status
	WorkerId  int
	StartTime time.Time
}

type Task struct {
	TaskId        int
	TaskPhase     TaskPhase
	Filename      string
	NumMapTask    int
	NumReduceTask int
	Alive         bool
}
