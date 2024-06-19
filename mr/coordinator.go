package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	taskList      chan *Task
	taskIndex     map[int]*Task
	status        int
	reduceNum     int
	files         []string
	intermediates [][]string
	outFile       []string
	lock          *sync.RWMutex
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		taskList:      make(chan *Task, max(len(files), nReduce)),
		taskIndex:     make(map[int]*Task),
		status:        Map,
		reduceNum:     nReduce,
		files:         files,
		intermediates: make([][]string, nReduce),
		outFile:       make([]string, nReduce),
		lock:          new(sync.RWMutex),
	}
	c.CreateMapTask()
	c.server()
	go c.CheckCrash()
	return &c
}

// CheckCrash 检测crash
func (c *Coordinator) CheckCrash() {
	for true {
		c.lock.Lock()
		if c.status == Exit {
			c.lock.Unlock()
			break
		}
		for i := range c.taskIndex {
			if ((c.taskIndex[i].TaskStage == Map) || (c.taskIndex[i].TaskStage == Reduce)) && (c.taskIndex[i].StartTime != time.Time{}) && (time.Since(c.taskIndex[i].StartTime).Seconds() >= 10) {
				c.taskIndex[i].StartTime = time.Time{}
				if c.status == Map {
					c.taskIndex[i].TaskStage = Map
				} else if c.status == Reduce {
					c.taskIndex[i].TaskStage = Reduce
				}
				c.taskList <- c.taskIndex[i]
			}
		}
		c.lock.Unlock()
		time.Sleep(time.Second * 2)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.lock.Lock()
	if len(c.taskList) > 0 {
		reply.Task = *<-c.taskList
		c.taskIndex[reply.Task.TaskId].StartTime = time.Now()
	} else {
		if c.status == Exit {
			reply.Task = Task{
				TaskStage: Exit,
				TaskId:    -1,
				ReduceNum: -1,
				FileName:  "",
			}
		} else {
			reply.Task = Task{
				TaskStage: Wait,
				TaskId:    -1,
				ReduceNum: -1,
				FileName:  "",
			}
		}
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	id, stage, files := args.TaskId, args.Stage, args.FilePaths
	c.lock.Lock()
	if stage != c.status || c.taskIndex[id].TaskStage == MapComplete || c.taskIndex[id].TaskStage == ReduceComplete {
		c.lock.Unlock()
		return nil
	}
	if stage == Map {
		c.taskIndex[id].TaskStage = MapComplete
		for i, v := range files {
			c.intermediates[i] = append(c.intermediates[i], v)
		}
	} else if stage == Reduce {
		c.taskIndex[id].TaskStage = ReduceComplete
		c.outFile[id] = files[0]
	}
	c.lock.Unlock()
	go c.CheckStage(stage)
	return nil
}

func (c *Coordinator) CheckStage(stage int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if stage == Map && c.isAllComplete() {
		c.status = Reduce
		c.CreateReduceTask()
	} else if stage == Reduce && c.isAllComplete() {
		c.status = Exit
	}
}

func (c *Coordinator) isAllComplete() bool {
	for i := range c.taskIndex {
		if c.taskIndex[i].TaskStage == Map || c.taskIndex[i].TaskStage == Reduce {

			return false
		}
	}
	return true
}

// use files to create tasks
func (c *Coordinator) CreateMapTask() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for i, f := range c.files {
		task := Task{
			TaskStage:     Map,
			TaskId:        i,
			ReduceNum:     c.reduceNum,
			Intermediates: nil,
			FileName:      f,
			StartTime:     time.Time{},
		}
		c.taskList <- &task
		c.taskIndex[i] = &task
	}
}

func (c *Coordinator) CreateReduceTask() {
	c.taskIndex = make(map[int]*Task)
	for i, files := range c.intermediates {
		task := Task{
			TaskStage:     Reduce,
			TaskId:        i,
			ReduceNum:     c.reduceNum,
			Intermediates: files,
			FileName:      "",
			StartTime:     time.Time{},
		}
		c.taskList <- &task
		c.taskIndex[i] = &task
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("tcp", ":1234")
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.status == Exit {
		time.Sleep(time.Second * 5)
		return true
	}
	return false
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
