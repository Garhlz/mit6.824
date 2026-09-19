package mr

type TaskType int

// Worker 向 Coordinator 请求的任务类型。
// 使用 const 与 iota 模拟枚举。
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
