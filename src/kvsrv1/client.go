package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// 可在此补充客户端状态。
	return ck
}

// Get 获取指定 key 的当前值与版本。key 不存在时返回 ErrNoKey；
// 遇到其他错误时持续重试。
//
// 可以按如下方式发送 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// args 与 reply 的类型（包括是否为指针）必须与 RPC handler 的参数声明一致，
// 且 reply 必须以指针形式传入。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// 在此实现客户端的 Get 重试逻辑。
	var getArgs rpc.GetArgs
	var getReply rpc.GetReply

	for {
		getArgs = rpc.GetArgs{Key: key}
		getReply = rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &getArgs, &getReply)

		// 通信失败，可能是请求丢失或者回复丢失，直接重试即可
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		break
	}
	if getReply.Err == rpc.OK {
		return getReply.Value, getReply.Version, getReply.Err
	}
	return "", 0, getReply.Err
}

// Put 仅在请求版本与服务端当前版本一致时更新 key。
// 版本不一致时，服务端返回 ErrVersion。若首次 RPC 就收到 ErrVersion，
// 可确定写入未执行；若重发后收到 ErrVersion，则先前请求可能已经成功但响应丢失，
// Clerk 无法确定写入结果，应向调用方返回 ErrMaybe。
//
// 可以按如下方式发送 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// args 与 reply 的类型（包括是否为指针）必须与 RPC handler 的参数声明一致，
// 且 reply 必须以指针形式传入。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// 在此实现带版本检查的 Put 重试逻辑。
	var putArgs rpc.PutArgs
	var putReply rpc.PutReply
	retry := 0
	for {
		putArgs = rpc.PutArgs{
			Key:     key,
			Value:   value,
			Version: version,
		}
		putReply = rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &putArgs, &putReply)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			retry += 1
			continue
		}
		break
	}

	if putReply.Err == rpc.ErrVersion {
		if retry == 0 {
			return rpc.ErrVersion
		}
		return rpc.ErrMaybe
	}

	return putReply.Err
}
