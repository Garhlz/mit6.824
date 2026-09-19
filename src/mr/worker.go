package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map 函数返回一组 KeyValue。
type KeyValue struct {
	Key   string
	Value string
}

// 使用 ihash(key) % NReduce，为 Map 产生的每个 KeyValue 选择 Reduce 任务。
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // Coordinator 的通信 socket

// main/mrworker.go 调用该函数启动 Worker。
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Worker 的主执行逻辑。
	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}

		// 主循环，调用coordinator的rpc方法，请求任务的内容
		ok := call("Coordinator.AskTask", &args, &reply)
		if ok {
			switch reply.Type {
			case MapTask:
				if err := handleMap(&reply, mapf); err != nil {
					log.Printf("map task %d failed: %v", reply.TaskID, err)
				}
			case ReduceTask:
				if err := handleReduce(&reply, reducef); err != nil {
					log.Printf("reduce task %d failed: %v", reply.TaskID, err)
				}
			case WaitTask:
				// 当前阶段还有任务没完成，但现在没有可分配的 idle task
				// 则休眠一段时间之后重新请求
				time.Sleep(time.Second)
			case ExitTask:
				return
			}
		} else {
			log.Printf("rpc failed, maybe coordinator has finished")
			return
		}
	}

}

// 向 Coordinator 发送 RPC 并等待响应。
// 调用成功时通常返回 true，发生通信错误时返回 false。
// 都是拉模型，worker向coordinator请求工作
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func handleMap(reply *AskTaskReply, mapf func(string, string) []KeyValue) error {
	if reply.NReduce <= 0 {
		return fmt.Errorf("map task %d: invalid reduce count %d", reply.TaskID, reply.NReduce)
	}

	content, err := os.ReadFile(reply.FileName)
	if err != nil {
		return fmt.Errorf("map task %d: read input %q: %w", reply.TaskID, reply.FileName, err)
	}
	intermediate := mapf(reply.FileName, string(content))

	result := make([][]KeyValue, reply.NReduce)
	// result[i] 保存第 i 个 Reduce 任务的中间结果。

	for _, kv := range intermediate {
		// 哈希之后取模的分区操作
		hashedKey := ihash(kv.Key) % reply.NReduce
		result[hashedKey] = append(result[hashedKey], kv)
	}

	for i := range result {
		finalName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		if err := writeIntermediateFile(finalName, result[i]); err != nil {
			return fmt.Errorf("map task %d: write partition %d: %w", reply.TaskID, i, err)
		}
	}

	// 任务完成后，通过另一个 RPC 向 Coordinator 汇报结果。
	reportTaskArgs := ReportTaskArgs{
		Type:    reply.Type,
		TaskID:  reply.TaskID,
		Attempt: reply.Attempt,
	}
	reportTaskReply := ReportTaskReply{}

	ok := call("Coordinator.ReportTask", &reportTaskArgs, &reportTaskReply)
	if !ok {
		return fmt.Errorf("report task error")
	}

	return nil
}

func handleReduce(reply *AskTaskReply, reducef func(string, []string) string) error {
	if reply.NMap < 0 {
		return fmt.Errorf("reduce task %d: invalid map count %d", reply.TaskID, reply.NMap)
	}

	intermediate := []KeyValue{}

	for i := range reply.NMap {
		// 契约：中间键值对写入此名称的文件中
		filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
		kva, err := readIntermediateFile(filename)
		if err != nil {
			return fmt.Errorf("reduce task %d: read intermediate file %q: %w", reply.TaskID, filename, err)
		}
		intermediate = append(intermediate, kva...)
	}

	// 按 key 排序，使相同 key 的记录连续排列。
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	tmp, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		return fmt.Errorf("reduce task %d: create temporary output: %w", reply.TaskID, err)
	}
	tmpName := tmp.Name()

	// 直接复制过来的
	// 对 intermediate 中的每个不同 key 调用 Reduce，
	// 并将结果写入 mr-out-{reduceID}。
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		// 收集连续区间内相同 key 的 value，再执行 Reduce。
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 按照要求进行写入
		if _, err := fmt.Fprintf(tmp, "%v %v\n", intermediate[i].Key, output); err != nil {
			// 如果写入失败，就删除半成品，且不能报告 reduce 完成
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return fmt.Errorf("reduce task %d: write temporary output: %w", reply.TaskID, err)
		}
		i = j
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("reduce task %d: close temporary output: %w", reply.TaskID, err)
	}

	// 先创建为临时随机名称，防止另一个worker进程发生冲突
	// 之后再修改为正确名称
	finalName := fmt.Sprintf("mr-out-%d", reply.TaskID)

	if err = os.Rename(tmpName, finalName); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("reduce task %d: publish output %q: %w", reply.TaskID, finalName, err)
	}

	reportTaskArgs := ReportTaskArgs{
		Type:    reply.Type,
		TaskID:  reply.TaskID,
		Attempt: reply.Attempt,
	}
	reportTaskReply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &reportTaskArgs, &reportTaskReply)
	if !ok {
		return fmt.Errorf("report task error")
	}

	return nil
}

func writeIntermediateFile(finalName string, kva []KeyValue) error {
	tmp, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	tmpName := tmp.Name()

	enc := json.NewEncoder(tmp)
	for _, kv := range kva {
		if err := enc.Encode(&kv); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return fmt.Errorf("encode key/value: %w", err)
		}
	}
	// ai在io部分弄了很多防御性编程
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close temporary file: %w", err)
	}

	if err := os.Rename(tmpName, finalName); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("publish %q: %w", finalName, err)
	}
	return nil
}

func readIntermediateFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var kva []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			_ = file.Close()
			return nil, fmt.Errorf("decode key/value: %w", err)
		}
		kva = append(kva, kv)
	}

	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close file: %w", err)
	}
	return kva, nil
}
