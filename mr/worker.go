package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerStatus := true
	for workerStatus {
		task := getTask()
		//sleep
		//rand
		//r := rand.New(rand.NewSource(time.Now().UnixNano()))
		//m := r.Intn(100)
		//if m%2 == 0 {
		//	time.Sleep(time.Second * 6)
		//	fmt.Printf("task %d is crash\n", task.TaskId)
		//	continue
		//} else {
		//	time.Sleep(time.Second * 3)
		//}
		//
		switch task.TaskStage {
		case Map:
			mapping(&task, mapf)
		case Reduce:
			reducing(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			workerStatus = false
		}
	}
	//log.Println("worker is done")
}

// rpc function
func getTask() Task {
	//GetTask RPC
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	//log.Println("get task ...")
	call("Coordinator.AssignTask", &args, &reply)
	//log.Println("get task ok:", reply.Task.TaskId, reply.Task.TaskStage)
	return reply.Task
}

// map&reduce
func mapping(t *Task, mapf func(string, string) []KeyValue) {
	//解析task
	content, err := os.ReadFile(t.FileName)
	if err != nil {
		log.Fatalf("cannot read %v", t.FileName)
	}
	//执行map过程
	kvList := mapf(t.FileName, string(content)) //filename,content
	//将kv散列到不同的文件中
	buffer := make([][]KeyValue, t.ReduceNum)
	for _, kv := range kvList {
		hash := ihash(kv.Key) % t.ReduceNum
		buffer[hash] = append(buffer[hash], kv)
	}
	//写入临时文件
	mapOutFiles := make([]string, t.ReduceNum)
	for i, v := range buffer {
		mapOutFiles[i] = WriteToTempFile(t.TaskId, i, v)
	}
	//发起complete RPC请求
	args, reply := CompleteTaskArgs{
		TaskId:    t.TaskId,
		Stage:     t.TaskStage,
		FilePaths: mapOutFiles,
	}, CompleteTaskReply{}
	call("Coordinator.CompleteTask", &args, &reply)
}

func reducing(t *Task, reducef func(string, []string) string) {
	//读取文件
	kvList := ReadFromTempFile(t.Intermediates)
	//shuffle
	sort.Slice(kvList, func(i, j int) bool {
		return kvList[i].Key <= kvList[j].Key
	})
	//执行reduce过程
	val := make([]string, 0)
	rep := make([]KeyValue, 0)
	var last KeyValue
	if len(kvList) > 0 {
		last = kvList[0]
		for _, kv := range kvList {
			if kv.Key != last.Key {
				rep = append(rep, KeyValue{last.Key, reducef(last.Key, val)})
				last = kv
				val = make([]string, 0)
			}
			val = append(val, kv.Value)
		}
		if len(val) != 0 {
			rep = append(rep, KeyValue{last.Key, reducef(last.Key, val)})
		}
	}
	//写入out文件
	filePath := WriteToOutFile(t.TaskId, rep)
	//发起complete RPC请求
	args, reply := CompleteTaskArgs{
		TaskId:    t.TaskId,
		Stage:     t.TaskStage,
		FilePaths: []string{filePath},
	}, CompleteTaskReply{}
	call("Coordinator.CompleteTask", &args, &reply)
}

func WriteToOutFile(x int, buf []KeyValue) string {
	dir, _ := os.Getwd()
	filePath := dir + "/" + "mr-out-" + strconv.Itoa(x)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer file.Close()
	for _, kv := range buf {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
	return filePath
}

func WriteToTempFile(x int, y int, buf []KeyValue) string {
	dir, _ := os.Getwd()
	filePath := dir + "/" + "mr-" + strconv.Itoa(x) + "-" + strconv.Itoa(y)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, kv := range buf {
		if err := enc.Encode(kv); err != nil {
			log.Fatal(kv, err.Error())
		}
	}
	return filePath
}

func ReadFromTempFile(files []string) []KeyValue {
	kvList := make([]KeyValue, 0)
	for _, fileName := range files {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err.Error())
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvList = append(kvList, kv)
		}
	}
	return kvList
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println("call", rpcname, "error: ", err.Error())
	return false
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
