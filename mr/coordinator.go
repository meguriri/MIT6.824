package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	taskList      chan *Task
	okSignal      chan int
	taskIndex     map[int]*Task
	status        int
	reduceNum     int
	files         []string
	intermediates [][]string
	outFile       []string
	lock          sync.RWMutex
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		taskList:      make(chan *Task, max(len(files), nReduce)),
		okSignal:      make(chan int),
		taskIndex:     make(map[int]*Task),
		status:        Map,
		reduceNum:     nReduce,
		files:         files,
		intermediates: make([][]string, nReduce),
		outFile:       make([]string, nReduce),
		lock:          sync.RWMutex{},
	}
	c.CreateMapTask()
	go c.CheckCrash()
	c.server()
	return &c
}

// CheckCrash 检测crash TODO
func (c *Coordinator) CheckCrash() {
	for c.status != Exit {
		//fmt.Println("-------Check Crash-------")
		c.lock.Lock()
		for i, _ := range c.taskIndex {
			//fmt.Printf("task[%v]:%v, ", c.taskIndex[i].TaskId, c.taskIndex[i].TaskStage)
			if ((c.taskIndex[i].TaskStage == Map) || (c.taskIndex[i].TaskStage == Reduce)) && (c.taskIndex[i].StartTime != time.Time{}) && (time.Since(c.taskIndex[i].StartTime).Seconds() > 10) {
				//fmt.Printf("------check crash task %d is crash------\n", c.taskIndex[i].TaskId)
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
		//fmt.Println("-------Check Done-------")
		time.Sleep(time.Second * 2)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.taskList) > 0 {
		reply.Task = *<-c.taskList
		//check time
		c.taskIndex[reply.Task.TaskId].StartTime = time.Now()
		//fmt.Printf("Assign task %d start time %v \n", reply.Task.TaskId, time.Now())
		//
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
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	id, stage, files := args.TaskId, args.Stage, args.FilePaths
	//check task is already done
	if stage != c.status || c.taskIndex[id].TaskStage == MapComplete || c.taskIndex[id].TaskStage == ReduceComplete {
		//log.Printf("task %d is already done\n", id)
		return nil
	}
	if stage == Map {
		c.taskIndex[id].TaskStage = MapComplete
		//log.Printf("Map task %d is ok,during %f second\n", id, time.Since(c.taskIndex[id].StartTime).Seconds())
		for i, v := range files {
			c.intermediates[i] = append(c.intermediates[i], v)
		}
	} else if stage == Reduce {
		//log.Printf("Reduce task %d is ok,during %f second\n", id, time.Since(c.taskIndex[id].StartTime).Seconds())
		c.taskIndex[id].TaskStage = ReduceComplete
		c.outFile[id] = files[0]
	}
	go c.CheckStage(stage)
	return nil
}

// 可以改进
func (c *Coordinator) CheckStage(stage int) {
	if stage == Map && c.isAllComplete() {
		//fmt.Println("Change to Reduce stages")
		c.status = Reduce
		c.CreateReduceTask()
		//fmt.Println("CreateReduceTask ok")
	} else if stage == Reduce && c.isAllComplete() {
		//fmt.Println("All complete")
		c.status = Exit
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
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
		// TODO
		time.Sleep(time.Second * 5)
		//
		return true
	}
	return false
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

func (c *Coordinator) isAllComplete() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	for i, _ := range c.taskIndex {
		if c.taskIndex[i].TaskStage == Map || c.taskIndex[i].TaskStage == Reduce {

			return false
		}
	}
	return true
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
