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

type KVServer struct {
	me  int
	rsm *rsm.RSM
	// 键值状态由 RSM 统一复制和驱动。
	mu    sync.Mutex
	kvmap map[string]*rpc.TValue
}

// 可使用 Go 的类型 switch 或类型断言将 req 转换为具体请求类型：
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
// rsm层调用这个DoOp和存储层交互
func (kv *KVServer) DoOp(req any) any {
	// 根据命令类型分派到对应的状态机操作。
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
		kv.kvmap[args.Key] = &rpc.TValue{
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
	kv.kvmap[args.Key] = &rpc.TValue{
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
	// 编码完整业务状态，供 Raft 创建快照。
	if data == nil {
		return
	}
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvmap map[string]*rpc.TValue
	if d.Decode(&kvmap) != nil {
		panic("restore snapshot error")
	} else {
		kv.mu.Lock()
		kv.kvmap = make(map[string]*rpc.TValue, len(kvmap))

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

// Get 由客户端调用，并通过 RSM 提交到 Raft。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// 使用 kv.rsm.Submit() 提交请求，并将 any 类型结果断言为 rpc.GetReply。
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
	// 使用 kv.rsm.Submit() 提交请求，并将 any 类型结果断言为 rpc.PutReply。
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

// StartKVServer() 和 MakeRSM() 必须快速返回，长期运行的任务应放入 goroutine。
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// 使用 labgob.Register 注册需要通过接口值编码的具体类型。
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:    me,
		mu:    sync.Mutex{},
		kvmap: make(map[string]*rpc.TValue),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// 在创建 RSM 前完成状态机的初始状态设置。
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
