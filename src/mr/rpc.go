package mr

type TaskType int

// worker向coordinator请求任务的不同类型
// 用const ... iota 模拟 enum
const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type AskTaskArgs struct{}

type AskTaskReply struct {
	Type     TaskType
	TaskID   int
	FileName string
	NMap     int
	NReduce  int
	Attempt  int
}

type ReportTaskArgs struct {
	Type    TaskType
	TaskID  int
	Attempt int
}

type ReportTaskReply struct{}
