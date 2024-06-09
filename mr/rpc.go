package mr

import "time"

// 定义任务
type Task struct {
	TaskStage     int       //任务的阶段
	TaskId        int       //任务的id
	ReduceNum     int       //reduce个数
	Intermediates []string  //Reduce时的临时文件
	FileName      string    //文件名称
	StartTime     time.Time //开始时间
}

// 任务类型
const (
	Map = iota
	MapComplete
	Reduce
	ReduceComplete
	Wait
	Exit
)

// 获取任务
type GetTaskArgs struct {
}

type GetTaskReply struct {
	Task Task
}

// 通知完成任务
type CompleteTaskArgs struct {
	TaskId    int
	Stage     int
	FilePaths []string
}
type CompleteTaskReply struct {
}
