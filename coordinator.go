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

type Coordinator struct {
	Files             []string
	IntermediateFiles [][]string
	OutputFiles       []string
	MapTasks          []Task
	ReduceTasks       []Task
	nReduce           int
	Lock              sync.Mutex
	mapTasksDone      bool
	reduceTasksDone   bool
	startTime         time.Time
}

type Task struct {
	TaskID    int
	WorkerID  string
	File      string
	Status    status
	Phase     phase
	StartTime time.Time
}

type status int
type phase int

const (
	Pending    status = 0
	InProgress status = 1
	Done       status = 2
)

const (
	MapPhase    phase = 0
	ReducePhase phase = 1
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestArgs, reply *ReplyArgs) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	for i := range c.MapTasks {
		if c.MapTasks[i].Status == Pending {
			c.MapTasks[i].Status = InProgress
			c.MapTasks[i].WorkerID = args.WorkerID
			c.MapTasks[i].StartTime = time.Now()
			reply.Task = c.MapTasks[i]
			reply.NReduce = c.nReduce
			return nil
		}
	}

	allMapDone := true
	for i := range c.MapTasks {
		if c.MapTasks[i].Status != Done {
			allMapDone = false
			break
		}
	}
	if allMapDone {
		c.mapTasksDone = true
	}

	if c.mapTasksDone && !c.reduceTasksDone {
		for i := range c.ReduceTasks {
			if c.ReduceTasks[i].Status == Pending {
				c.ReduceTasks[i].Status = InProgress
				c.ReduceTasks[i].WorkerID = args.WorkerID
				c.ReduceTasks[i].StartTime = time.Now()
				reply.Task = c.ReduceTasks[i]
				reply.NReduce = c.nReduce
				return nil
			}
		}
	}

	allReduceDone := true
	for i := range c.ReduceTasks {
		if c.ReduceTasks[i].Status != Done {
			allReduceDone = false
			break
		}
	}
	if allReduceDone {
		c.reduceTasksDone = true
	}

	return fmt.Errorf("no tasks available")
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.Lock.Lock()
	defer c.Lock.Unlock()

	if c.mapTasksDone && c.reduceTasksDone || time.Since(c.startTime).Seconds() >= 120 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:           files,
		nReduce:         nReduce,
		MapTasks:        make([]Task, len(files)),
		ReduceTasks:     make([]Task, nReduce),
		mapTasksDone:    false,
		reduceTasksDone: false,
		startTime:       time.Now(),
	}

	c.IntermediateFiles = c.createIntermediateFiles(files, nReduce)
	c.OutputFiles = c.createOutputFiles(nReduce)

	// initialize map tasks
	for i, file := range files {
		c.MapTasks[i] = Task{
			TaskID: i,
			File:   file,
			Phase:  MapPhase,
			Status: Pending,
		}
	}

	// initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{
			TaskID: i,
			File:   c.OutputFiles[i],
			Phase:  ReducePhase,
			Status: Pending,
		}
	}

	c.server()
	go c.monitorTasks()
	return &c
}

func (c *Coordinator) monitorTasks() {
	for {
		time.Sleep(5 * time.Second)
		c.Lock.Lock()
		for i := range c.MapTasks {
			task := &c.MapTasks[i]
			if task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second {
				// fmt.Printf("Map Task %d by worker %s timed out. Reassigning...\n", task.TaskID, task.WorkerID)
				task.Status = Pending
				task.StartTime = time.Time{}
			}
		}
		for i := range c.ReduceTasks {
			task := &c.ReduceTasks[i]
			if task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second {
				// fmt.Printf("Reduce Task %d by worker %s timed out. Reassigning...\n", task.TaskID, task.WorkerID)
				task.Status = Pending
				task.StartTime = time.Time{}
			}
		}
		c.Lock.Unlock()
	}
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	updated := false
	for i := range c.MapTasks {
		if c.MapTasks[i].TaskID == args.TaskID && c.MapTasks[i].WorkerID == args.WorkerID {
			c.MapTasks[i].Status = Done
			updated = true
			break
		}
	}
	if !updated {
		for i := range c.ReduceTasks {
			if c.ReduceTasks[i].TaskID == args.TaskID && c.ReduceTasks[i].WorkerID == args.WorkerID {
				c.ReduceTasks[i].Status = Done
				updated = true
				break
			}
		}
	}

	if !updated {
		return fmt.Errorf("Task ID %d not found for worker ID %s", args.TaskID, args.WorkerID)
	}

	// fmt.Printf("Task %d reported as done by worker %s\n", args.TaskID, args.WorkerID)
	return nil
}

func (c *Coordinator) createIntermediateFiles(files []string, nReduce int) [][]string {
	fileNames := make([][]string, len(files))
	for i, filePath := range files {
		base := filepath.Base(filePath)                      // remove "../data/"
		name := strings.TrimSuffix(base, filepath.Ext(base)) // remove ".txt"
		fileNames[i] = make([]string, nReduce)
		for j := range fileNames[i] {
			fileName := fmt.Sprintf("m-%s-%d.txt", name, j)
			file, err := os.Create(fileName)
			if err != nil {
				panic(err)
			}
			file.Close()
			fileNames[i][j] = fileName
		}
	}
	return fileNames
}

func (c *Coordinator) createOutputFiles(nReduce int) []string {
	fileNames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-out-%d.txt", i)
		file, err := os.Create(fileName)
		if err != nil {
			panic(err)
		}
		file.Close()
		fileNames[i] = fileName
	}
	return fileNames
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
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
