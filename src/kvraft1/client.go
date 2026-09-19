package kvraft

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // 最近一次成功响应的 leader 在 servers[] 中的索引
	// 可在此补充 Clerk 状态。
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, leader: 0}
	// 初始化 Clerk 的 leader 提示信息。
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) tryNext(leader int) int {
	length := len(ck.servers)
	leader++
	leader %= length
	return leader
}

// Get 获取指定 key 的当前值与版本。key 不存在时返回 ErrNoKey；
// 遇到其他错误时在各 Raft 节点间持续重试。
//
// 可以按如下方式向第 i 个服务器发送 RPC：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// args 与 reply 的类型（包括是否为指针）必须与 RPC handler 的参数声明一致，
// 且 reply 必须以指针形式传入。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// 在此实现寻找 leader 与重试逻辑。
	var getArgs rpc.GetArgs
	var getReply rpc.GetReply
	leader := ck.leader
	for {
		getArgs = rpc.GetArgs{Key: key}
		getReply = rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &getArgs, &getReply)

		// 通信失败，leader错误
		if !ok {
			leader = ck.tryNext(leader)
			continue
		}

		switch getReply.Err {
		case rpc.ErrWrongLeader:
			leader = ck.tryNext(leader)
			continue
		case rpc.OK:
			ck.leader = leader
			return getReply.Value, getReply.Version, getReply.Err
		case rpc.ErrNoKey:
			ck.leader = leader
			return "", 0, getReply.Err
		default:
			leader = ck.tryNext(leader)
			continue
		}
	}
}

// Put 仅在请求版本与服务端当前版本一致时更新 key。
// 首次 RPC 返回 ErrVersion 表示写入确定未执行；若重发后返回 ErrVersion，
// 先前请求可能已成功但响应丢失，此时 Clerk 应返回 ErrMaybe。
//
// 可以按如下方式向第 i 个服务器发送 RPC：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// args 与 reply 的类型（包括是否为指针）必须与 RPC handler 的参数声明一致，
// 且 reply 必须以指针形式传入。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// 在此实现带版本语义的 Put 重试逻辑。
	var putArgs rpc.PutArgs
	var putReply rpc.PutReply
	retry := 0
	leader := ck.leader
	for {
		putArgs = rpc.PutArgs{
			Key:     key,
			Value:   value,
			Version: version,
		}
		putReply = rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &putArgs, &putReply)
		if !ok {
			leader = ck.tryNext(leader)
			retry++
			continue
		}
		switch putReply.Err {
		case rpc.ErrWrongLeader:
			leader = ck.tryNext(leader)
			retry++
			continue
		case rpc.ErrVersion:
			ck.leader = leader
			if retry == 0 {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		case rpc.OK:
			ck.leader = leader
			return rpc.OK
		case rpc.ErrNoKey:
			ck.leader = leader
			return rpc.ErrNoKey
		default:
			leader = ck.tryNext(leader)
			continue
		}

	}
}
