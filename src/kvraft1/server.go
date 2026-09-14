package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type TValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	mu    sync.Mutex
	kvmap map[string]*TValue
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	if args, ok := req.(rpc.GetArgs); ok {
		return kv.doGet(args)
	} else if args, ok := req.(rpc.PutArgs); ok {
		return kv.doPut(args)
	}
	return nil
}

func (kv *KVServer) doGet(args rpc.GetArgs) rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := rpc.GetReply{}
	value, ok := kv.kvmap[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return reply
	}
	reply.Value = value.Value
	reply.Version = value.Version
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) doPut(args rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := rpc.PutReply{}
	value, ok := kv.kvmap[args.Key]
	// 不存在这个key
	if !ok {
		// 参数的版本号大于零
		if args.Version > 0 {
			reply.Err = rpc.ErrNoKey
			return reply
		}

		// 版本号 = 0
		kv.kvmap[args.Key] = &TValue{
			Value:   args.Value,
			Version: 1,
		}
		reply.Err = rpc.OK
		return reply
	}

	if args.Version != value.Version {
		reply.Err = rpc.ErrVersion
		return reply
	}

	// key存在且版本号正确的路径
	kv.kvmap[args.Key] = &TValue{
		Value:   args.Value,
		Version: args.Version + 1,
	}

	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.mu.Lock()
	e.Encode(kv.kvmap)
	kv.mu.Unlock()

	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if data == nil {
		return
	}
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvmap map[string]*TValue
	if d.Decode(&kvmap) != nil {
		panic("restore snapshot error")
	} else {
		kv.mu.Lock()
		kv.kvmap = make(map[string]*TValue, len(kvmap))

		for k, v := range kvmap {
			if v == nil {
				kv.kvmap[k] = nil
				continue
			}
			// 因为这里值是指针，需要把指针指向的对象拷贝出来
			copied := *v
			kv.kvmap[k] = &copied
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	localArgs := rpc.GetArgs{
		Key: args.Key,
	}
	err, result := kv.rsm.Submit(localArgs)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	currentReply, ok := result.(rpc.GetReply)
	if !ok {
		panic("internal error")
	}
	reply.Err = currentReply.Err
	reply.Value = currentReply.Value
	reply.Version = currentReply.Version

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	localArgs := rpc.PutArgs{
		Key:     args.Key,
		Value:   args.Value,
		Version: args.Version,
	}
	err, result := kv.rsm.Submit(localArgs)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	currentReply, ok := result.(rpc.PutReply)
	if !ok {
		panic("internal error")
	}
	reply.Err = currentReply.Err
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:    me,
		mu:    sync.Mutex{},
		kvmap: make(map[string]*TValue),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
