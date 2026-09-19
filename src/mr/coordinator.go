package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 单个任务的状态
type TaskState int

const (
	Idle TaskState = iota
	Running
	Done
)

type TaskMeta struct {
	StartTime time.Time
	State     TaskState
	Attempt   int
}

// coordinator整体的状态
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	Finished
)

type Coordinator struct {
	mu sync.Mutex

	files   []string
	nReduce int

	mapTasks    []TaskMeta
	reduceTasks []TaskMeta

	phase Phase
}

// 启动 RPC 服务，监听 worker.go 发来的请求。
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go 会定期调用 Done()，检查整个作业是否已经完成。
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.phase == Finished

	return ret
}

// MakeCoordinator 创建协调器，由 main/mrcoordinator.go 调用。
// nReduce 指定 Reduce 任务的数量。
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	if nReduce <= 0 {
		panic("mr: nReduce must be positive")
	}

	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]TaskMeta, len(files)),
		reduceTasks: make([]TaskMeta, nReduce),
		phase:       MapPhase,
		mu:          sync.Mutex{}}

	// 零值应该恰好就是Idle，所以不需要初始化
	// for i := range len(files) {
	// 	c.mapTasks[i].State = Idle
	// }

	// for i := range nReduce {
	// 	c.reduceTasks[i].State = Idle
	// }

	c.server(sockname)
	return &c
}

func (c *Coordinator) setTimeout() {
	if c.phase == MapPhase {
		for i := range len(c.mapTasks) {
			task := &c.mapTasks[i]

			if task.State == Running &&
				time.Since(task.StartTime) > 10*time.Second {
				task.State = Idle
				task.Attempt += 1
			}
		}
	} else if c.phase == ReducePhase {
		for i := range len(c.reduceTasks) {
			task := &c.reduceTasks[i]

			if task.State == Running &&
				time.Since(task.StartTime) > 10*time.Second {
				task.State = Idle
				task.Attempt += 1
			}
		}
	}
}

func (c *Coordinator) checkPhaseFinished() bool {
	switch c.phase {
	case MapPhase:
		allDone := true
		// 或许可以维护done的任务数量，和总数比较
		for _, task := range c.mapTasks {
			if task.State != Done {
				allDone = false
				break
			}
		}
		return allDone
	case ReducePhase:

		allDone := true
		for _, task := range c.reduceTasks {
			if task.State != Done {
				allDone = false
				break
			}
		}
		return allDone
	default:
		return false
	}
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.setTimeout()
	switch c.phase {
	case MapPhase:
		foundIdle := false
		for i := range c.mapTasks {
			task := &c.mapTasks[i]
			if task.State == Idle {
				reply.FileName = c.files[i]
				reply.NMap = len(c.files)
				reply.NReduce = c.nReduce
				reply.TaskID = i
				reply.Type = MapTask
				reply.Attempt = task.Attempt
				foundIdle = true
				task.StartTime = time.Now()
				task.State = Running
				break
			}
		}

		if !foundIdle {
			if !c.checkPhaseFinished() {
				reply.Type = WaitTask
			} else {
				reply.Type = WaitTask
				c.phase = ReducePhase
			}
		}
	case ReducePhase:
		foundIdle := false
		for i := range c.reduceTasks {
			task := &c.reduceTasks[i]
			if task.State == Idle {
				reply.NMap = len(c.files)
				reply.NReduce = c.nReduce
				reply.TaskID = i
				reply.Type = ReduceTask
				reply.Attempt = task.Attempt
				foundIdle = true
				task.StartTime = time.Now()
				task.State = Running
				break
			}
		}

		if !foundIdle {
			if !c.checkPhaseFinished() {
				reply.Type = WaitTask
			} else {
				reply.Type = ExitTask
				c.phase = Finished
			}
		}
	case Finished:
		reply.Type = ExitTask
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case MapPhase:
		if args.Type != MapTask {
			return errors.New("error phase")
		}
		if args.TaskID < 0 || args.TaskID >= len(c.mapTasks) {
			return fmt.Errorf("invalid map task ID %d", args.TaskID)
		}
		if args.Attempt != c.mapTasks[args.TaskID].Attempt {
			return errors.New("out of date attempt")
		}
		if c.mapTasks[args.TaskID].State == Running {
			c.mapTasks[args.TaskID].State = Done
		}
		if c.checkPhaseFinished() {
			c.phase = ReducePhase
		}
	case ReducePhase:
		if args.Type != ReduceTask {
			return errors.New("error phase")
		}
		if args.TaskID < 0 || args.TaskID >= len(c.reduceTasks) {
			return fmt.Errorf("invalid reduce task ID %d", args.TaskID)
		}
		if args.Attempt != c.reduceTasks[args.TaskID].Attempt {
			return errors.New("out of date attempt")
		}
		if c.reduceTasks[args.TaskID].State == Running {
			c.reduceTasks[args.TaskID].State = Done
		}
		if c.checkPhaseFinished() {
			c.phase = Finished
		}
	}

	return nil
}
