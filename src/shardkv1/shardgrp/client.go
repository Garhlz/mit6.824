package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // 最近一次成功响应的 leader 在 servers[] 中的索引
	// 可在此补充 shardgrp Clerk 状态。
}

// 当前client是和一个raft集群（也就是shard group）通信的client
// 任务是找到raft leader，发送rpc请求
func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) nextLeader(leader int) int {
	length := len(ck.servers)
	leader++
	leader %= length
	return leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	var getArgs rpc.GetArgs
	var getReply rpc.GetReply
	leader := ck.leader
	attempts := 0
	for {
		getArgs = rpc.GetArgs{Key: key}
		getReply = rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &getArgs, &getReply)

		// 通信失败，尝试下个leader
		if !ok {
			leader = ck.nextLeader(leader)
		} else {
			switch getReply.Err {
			case rpc.ErrWrongLeader:
				leader = ck.nextLeader(leader)
			case rpc.OK:
				ck.leader = leader
				return getReply.Value, getReply.Version, getReply.Err
			case rpc.ErrNoKey:
				ck.leader = leader
				return "", 0, rpc.ErrNoKey
			case rpc.ErrWrongGroup: // 先在rsm层判断是否wrong leader，再在kvserer层判断的是否wrong group
				ck.leader = leader
				return "", 0, rpc.ErrWrongGroup
			default:
				leader = ck.nextLeader(leader)
			}
		}
		attempts++
		// 所有服务器都查询过一遍，让调用者重新查询配置
		if attempts >= len(ck.servers) {
			return "", 0, rpc.ErrWrongGroup
		}
		//  每轮重试短暂休眠，避免所有服务器不可用时忙循环
		time.Sleep(100 * time.Millisecond)

	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	var putArgs rpc.PutArgs
	var putReply rpc.PutReply
	attempts := 0
	uncertain := false
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
			leader = ck.nextLeader(leader)
			// 只有明确通信失败的时候才有可能产生err maybe
			uncertain = true
		} else {
			switch putReply.Err {
			case rpc.ErrWrongLeader:
				// Start 接收了请求之后、等待结果期间身份改变，不一定已经提交
				uncertain = true
				leader = ck.nextLeader(leader)
			case rpc.ErrVersion:
				ck.leader = leader
				if !uncertain {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			case rpc.OK:
				ck.leader = leader
				return rpc.OK
			case rpc.ErrNoKey:
				ck.leader = leader
				return rpc.ErrNoKey
			case rpc.ErrWrongGroup:
				ck.leader = leader
				if uncertain {
					return rpc.ErrMaybe
				}
				return rpc.ErrWrongGroup
			default:
				leader = ck.nextLeader(leader)
			}

		}
		attempts++
		// 已经全部尝试过了，可能是组不对，也有可能已经成功但是响应丢失了
		if attempts >= len(ck.servers) {
			if uncertain {
				return rpc.ErrMaybe
			}
			return rpc.ErrWrongGroup
		}
		//  每轮重试短暂休眠，避免所有服务器不可用时忙循环
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	var args shardrpc.FreezeShardArgs
	var reply shardrpc.FreezeShardReply
	leader := ck.leader
	attempts := 0
	for {
		args = shardrpc.FreezeShardArgs{
			Shard: s,
			Num:   num,
		}
		reply = shardrpc.FreezeShardReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.FreezeShard", &args, &reply)
		if !ok {
			leader = ck.nextLeader(leader)
		} else {
			switch reply.Err {
			case rpc.OK:
				ck.leader = leader
				return reply.State, rpc.OK
			case rpc.ErrWrongLeader:
				leader = ck.nextLeader(leader)
			case rpc.ErrWrongGroup:
				return []byte{}, rpc.ErrWrongGroup
			default:
			}
		}
		attempts++
		if attempts >= len(ck.servers)*2 {
			return []byte{}, rpc.ErrMaybe
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	var args shardrpc.InstallShardArgs
	var reply shardrpc.InstallShardReply
	leader := ck.leader
	attempts := 0
	for {
		args = shardrpc.InstallShardArgs{
			Shard: s,
			State: state,
			Num:   num,
		}
		reply = shardrpc.InstallShardReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.InstallShard", &args, &reply)
		if !ok {
			leader = ck.nextLeader(leader)
		} else {
			switch reply.Err {
			case rpc.OK:
				ck.leader = leader
				return rpc.OK
			case rpc.ErrWrongLeader:
				leader = ck.nextLeader(leader)
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			default:
			}
		}
		attempts++
		if attempts >= len(ck.servers)*2 {
			return rpc.ErrMaybe
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	var args shardrpc.DeleteShardArgs
	var reply shardrpc.DeleteShardReply
	leader := ck.leader
	attempts := 0
	for {
		args = shardrpc.DeleteShardArgs{
			Shard: s,
			Num:   num,
		}
		reply = shardrpc.DeleteShardReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.DeleteShard", &args, &reply)
		if !ok {
			leader = ck.nextLeader(leader)
		} else {
			switch reply.Err {
			case rpc.OK:
				ck.leader = leader
				return rpc.OK
			case rpc.ErrWrongLeader:
				leader = ck.nextLeader(leader)
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup
			default:
			}
		}
		attempts++
		if attempts >= len(ck.servers)*2 {
			return rpc.ErrMaybe
		}
		time.Sleep(100 * time.Millisecond)
	}
}
