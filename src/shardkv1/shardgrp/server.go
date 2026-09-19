package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type ShardMeta struct {
	LastNum shardcfg.Tnum
	Frozen  bool
	Owned   bool
	// 删除后仍保留 LastNum，用于拒绝迟到的旧 RPC；删除仅将 Owned 置为 false。
}

// 迁移 RPC 可能延迟或乱序到达，因此每个 shard 都要保存配置进度。
type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid // 当前集群对应的shard group id
	// kvmap 与 shards 共同组成需要由 RSM 复制和快照的业务状态。
	mu sync.Mutex
	// 当前服务器的键值对存储
	kvmap  map[string]*rpc.TValue
	shards map[shardcfg.Tshid]ShardMeta
}

// StartServerShardGrp 启动 gid 对应 shardgrp 中的一个服务器。
//
// StartServerShardGrp() 和 MakeRSM() 必须快速返回，长期任务应放入 goroutine。
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// 注册需要经由 labgob 接口值编码的具体命令类型。
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:    gid,
		me:     me,
		mu:     sync.Mutex{},
		kvmap:  map[string]*rpc.TValue{},
		shards: map[shardcfg.Tshid]ShardMeta{},
	}
	// 第一个组 Gid1 初始拥有所有 shard
	if kv.gid == shardcfg.Gid1 {
		for i := range shardcfg.NShards {
			kv.shards[shardcfg.Tshid(i)] = ShardMeta{
				LastNum: 0,
				Frozen:  false,
				Owned:   true,
			}
		}
	}
	// make rsm之后，raft可能会请求restore
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}

// rsm层调用DoOp和存储层交互
// todo 可以优化，get只从leader处获取
func (kv *KVServer) DoOp(req any) any {
	// 根据命令类型分派到对应的状态机操作。
	if args, ok := req.(rpc.GetArgs); ok {
		return kv.handleGet(args)
	} else if args, ok := req.(rpc.PutArgs); ok {
		return kv.handlePut(args)
	} else if args, ok := req.(shardrpc.FreezeShardArgs); ok {
		return kv.handleFreezeShard(args)
	} else if args, ok := req.(shardrpc.InstallShardArgs); ok {
		return kv.handleInstallShard(args)
	} else if args, ok := req.(shardrpc.DeleteShardArgs); ok {
		return kv.handleDeleteShard(args)
	}
	return nil
}

// 这里既然是rsm发过来的请求，默认之前已经过滤掉了
func (kv *KVServer) handleGet(args rpc.GetArgs) rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := rpc.GetReply{}

	// 先判断对应shard是否在当前group中
	// shard 的归属、冻结状态和配置编号都是被 Raft 复制的状态，所以需要在raft处理之后再判断
	shardID := shardcfg.Key2Shard(args.Key)
	shard, ok := kv.shards[shardID]
	if !ok || !shard.Owned {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	// 当前尝试get的key所在shard被冻结了
	if kv.shards[shardID].Frozen {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	value, ok := kv.kvmap[args.Key]
	// key不存在
	if !ok {
		reply.Err = rpc.ErrNoKey
		return reply
	}
	reply.Value = value.Value
	reply.Version = value.Version
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) handlePut(args rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := rpc.PutReply{}

	shardID := shardcfg.Key2Shard(args.Key)
	shard, ok := kv.shards[shardID]
	if !ok || !shard.Owned {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	// 当前尝试put的key所在shard被冻结了
	if kv.shards[shardID].Frozen {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	// 之后就是正常put的过程
	value, ok := kv.kvmap[args.Key]
	// key不存在
	if !ok {
		// 参数的版本号大于零，并非新建
		if args.Version > 0 {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		// 传入的版本号 = 0（TVersion是uint64），表示新建一个键值对
		kv.kvmap[args.Key] = &rpc.TValue{
			Value:   args.Value,
			Version: 1,
		}
		reply.Err = rpc.OK
		return reply
	}

	// 版本号不匹配
	if args.Version != value.Version {
		reply.Err = rpc.ErrVersion
		return reply
	}

	// key存在且版本号匹配
	kv.kvmap[args.Key] = &rpc.TValue{
		Value:   args.Value,
		Version: args.Version + 1, // 更新版本号
	}
	reply.Err = rpc.OK
	return reply
}

// 从kvserver中获取shard的数据并对其编码
// 持锁调用
func (kv *KVServer) getShardData(shid shardcfg.Tshid) []byte {
	shardkv := map[string]*rpc.TValue{}
	// 重复请求，保持幂等性，重新返回一遍数据
	for k, v := range kv.kvmap {
		shardID := shardcfg.Key2Shard(k)
		if shardID != shid {
			continue
		}
		shardkv[k] = v
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(shardkv)
	data := w.Bytes()
	return data
}

// 经过了raft共识之后打到kvserver的shard相关请求
func (kv *KVServer) handleFreezeShard(args shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := shardrpc.FreezeShardReply{}

	// 先判断对应shard是否在当前group中
	shard, ok := kv.shards[args.Shard]
	// 如果上次已经完成了之前的删除操作，直接返回成功即可
	if ok {
		if shard.LastNum == args.Num {
			if !shard.Owned {
				// 之前完成了delete
				reply.Err = rpc.OK
				reply.Num = args.Num
				reply.State = []byte{}
				return reply
			}
			if shard.Frozen && shard.Owned {
				// 之前完成了freeze，但是不知道是否install，依然携带数据返回
				reply.Err = rpc.OK
				reply.Num = args.Num
				reply.State = append([]byte{}, kv.getShardData(args.Shard)...)
				return reply
			}
		}
	}
	// 不存在，或者之前删除过（但是num不同）
	if !ok || !shard.Owned {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	num := kv.shards[args.Shard].LastNum

	if args.Num < num {
		// 表明这个请求过时了
		reply.Err = rpc.ErrWrongGroup
		reply.Num = num
		return reply
	}

	if args.Num > num {
		// 新的请求
		kv.shards[args.Shard] = ShardMeta{
			LastNum: args.Num,
			Frozen:  true,
			Owned:   true, // 删除之后才算不拥有
		}

	}
	// args.Num == num 重复请求，保持幂等性，也要重新返回一遍数据
	reply.State = append([]byte{}, kv.getShardData(args.Shard)...)
	reply.Num = args.Num
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) handleInstallShard(args shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := shardrpc.InstallShardReply{}

	num := kv.shards[args.Shard].LastNum

	if args.Num < num {
		// 这个请求过时了
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	// 如果是同编号install，可能会覆盖新写入的数据
	// 或者有可能是已经删除成功返回的空数据
	// 直接返回成功
	if args.Num == num && kv.shards[args.Shard].Owned {
		reply.Err = rpc.OK
		return reply
	}
	r := bytes.NewBuffer(args.State)
	d := labgob.NewDecoder(r)
	var shardkv map[string]*rpc.TValue

	if d.Decode(&shardkv) != nil {
		panic("restore snapshot error")
	} else {
		// shard 已冻结，安装的是该冻结时刻的一致数据，无需再次比较 value 版本。
		for k, v := range shardkv {
			kv.kvmap[k] = v
		}
	}

	kv.shards[args.Shard] = ShardMeta{
		LastNum: args.Num,
		Frozen:  false,
		Owned:   true, // 安装完成后，本组正式持有该 shard
	}
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) handleDeleteShard(args shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := shardrpc.DeleteShardReply{}

	// freeze之后依然存在且拥有
	shard, ok := kv.shards[args.Shard]
	if ok {
		// 已经删除过
		if args.Num == kv.shards[args.Shard].LastNum && !shard.Owned {
			reply.Err = rpc.OK
			return reply
		}
	}

	if !ok || !shard.Owned {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	num := kv.shards[args.Shard].LastNum
	if args.Num < num {
		// 这个请求过时了
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	for k := range kv.kvmap {
		if shardID := shardcfg.Key2Shard(k); shardID == args.Shard {
			delete(kv.kvmap, k)
		}
	}
	kv.shards[args.Shard] = ShardMeta{
		LastNum: args.Num,
		Frozen:  false,
		Owned:   false,
	}

	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.mu.Lock()
	e.Encode(kv.kvmap)
	e.Encode(kv.shards)
	kv.mu.Unlock()

	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) Restore(data []byte) {
	// 空快照表示没有可恢复状态，保留启动时的初始值。
	if data == nil {
		return
	}
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvmap map[string]*rpc.TValue
	var shards map[shardcfg.Tshid]ShardMeta

	if d.Decode(&kvmap) != nil || d.Decode(&shards) != nil {
		panic("restore snapshot error")
	} else {
		kv.mu.Lock()
		kv.kvmap = kvmap
		kv.shards = shards

		kv.mu.Unlock()
	}
}

// 这两个方法是当前shard group对应的client通过rpc调用的
// todo 是这里可以优化，使get请求不经过raft共识，只从leader中获取
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	rsmArgs := rpc.GetArgs{
		Key: args.Key,
	}
	err, result := kv.rsm.Submit(rsmArgs)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	rsmReply, ok := result.(rpc.GetReply)
	if !ok {
		panic("internal error")
	}
	reply.Err = rsmReply.Err
	reply.Value = rsmReply.Value
	reply.Version = rsmReply.Version

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	rsmArgs := rpc.PutArgs{
		Key:     args.Key,
		Value:   args.Value,
		Version: args.Version,
	}
	err, result := kv.rsm.Submit(rsmArgs)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	rsmReply, ok := result.(rpc.PutReply)
	if !ok {
		panic("internal error")
	}
	reply.Err = rsmReply.Err
}

// FreezeShard 冻结指定 shard，拒绝后续 Get/Put，并返回该 shard 的键值状态。
// 该 RPC 由源组对应的 shardgrp Clerk 调用。
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {

	rsmArgs := shardrpc.FreezeShardArgs{
		Shard: args.Shard,
		Num:   args.Num,
	}
	err, result := kv.rsm.Submit(rsmArgs)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	rsmReply, ok := result.(shardrpc.FreezeShardReply)
	if !ok {
		panic("internal error")
	}
	reply.Err = rsmReply.Err
	reply.Num = rsmReply.Num
	reply.State = append([]byte{}, rsmReply.State...)
}

// InstallShard 将传入状态安装到目标 shard；重复安装必须保持幂等。
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	rsmArgs := shardrpc.InstallShardArgs{
		Shard: args.Shard,
		State: append([]byte{}, args.State...),
		Num:   args.Num,
	}
	err, result := kv.rsm.Submit(rsmArgs)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	rsmReply, ok := result.(shardrpc.InstallShardReply)
	if !ok {
		panic("internal error")
	}
	reply.Err = rsmReply.Err
}

// DeleteShard 删除已迁出的 shard；同一配置编号的重复删除视为成功。
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	rsmArgs := shardrpc.DeleteShardArgs{
		Shard: args.Shard,
		Num:   args.Num,
	}
	err, result := kv.rsm.Submit(rsmArgs)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	rsmReply, ok := result.(shardrpc.DeleteShardReply)
	if !ok {
		panic("internal error")
	}
	reply.Err = rsmReply.Err
}
