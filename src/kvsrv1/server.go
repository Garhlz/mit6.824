package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type TValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	kvmap map[string]*TValue
	// 键值服务的内存状态。

}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu:    sync.Mutex{},
		kvmap: make(map[string]*TValue),
	}
	// 初始化服务端状态。

	return kv
}

// Get 返回 args.Key 对应的值与版本；key 不存在时返回 ErrNoKey。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// 在锁内读取键值状态。
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.kvmap[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = value.Value
	reply.Version = value.Version
	reply.Err = rpc.OK
}

// Put 仅在 args.Version 与服务端版本一致时更新 key；版本不匹配时返回
// ErrVersion。若 key 不存在，仅当 args.Version 为 0 时创建，否则返回 ErrNoKey。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// 在锁内执行带版本条件的写入。
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.kvmap[args.Key]
	// 不存在这个key
	if !ok {
		// 参数的版本号大于零
		if args.Version > 0 {
			reply.Err = rpc.ErrNoKey
			return
		}

		// 版本号 = 0
		kv.kvmap[args.Key] = &TValue{
			Value:   args.Value,
			Version: 1,
		}
		reply.Err = rpc.OK
		return
	}

	if args.Version != value.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	// key存在且版本号正确的路径
	kv.kvmap[args.Key] = &TValue{
		Value:   args.Value,
		Version: args.Version + 1,
	}

	reply.Err = rpc.OK
}

// 这些参数供复制式 KV 服务使用，本实验的单节点服务可以忽略。
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
