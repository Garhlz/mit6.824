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
	// Your definitions here.

}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu:    sync.Mutex{},
		kvmap: make(map[string]*TValue),
	}
	// Your code here.

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
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

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
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

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
