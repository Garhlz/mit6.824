package shardkv

//
// 用于访问分片键值服务的客户端代码。
//
// 客户端通过 shardctrler 查询当前配置，确定 key 所属 shard 及其负责组，
// 再向对应 shardgrp 发起请求。这是整个分片键值系统对外暴露的 Clerk。

import (
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// rcks 缓存各 gid 对应的 shardgrp Clerk；配置本身仍以 controller 为准。
}

// 测试程序调用 MakeClerk，并传入可供客户端调用 Query 的 shardctrler。
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get 先用 shardcfg.Key2Shard(key) 定位 shard，再查询当前配置找到负责组。
// 对应组的 Clerk 可通过 shardgrp.MakeClerk(ck.clnt, servers) 创建。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shardID := shardcfg.Key2Shard(key)
	for {
		config := ck.sck.Query()
		groupID := config.Shards[shardID]
		groupClerk, ok := ck.rcks[groupID]
		if !ok {
			// 第一次请求的时候惰性创建group clerk
			ck.rcks[groupID] = shardgrp.MakeClerk(ck.clnt, config.Groups[groupID])
			groupClerk = ck.rcks[groupID]
		}
		value, version, err := groupClerk.Get(key)
		switch err {
		case rpc.OK, rpc.ErrNoKey:
			return value, version, err
		case rpc.ErrWrongGroup:
			// 重试，重新查询获取新配置
			continue
		default:
		}
	}
}

// Put 将键值写入当前负责该 shard 的组。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	uncertain := false
	for {
		shardID := shardcfg.Key2Shard(key)
		config := ck.sck.Query()
		groupID := config.Shards[shardID]
		groupClerk, ok := ck.rcks[groupID]
		if !ok {
			ck.rcks[groupID] = shardgrp.MakeClerk(ck.clnt, config.Groups[groupID])
			groupClerk = ck.rcks[groupID]
		}
		err := groupClerk.Put(key, value, version)
		switch err {
		case rpc.OK, rpc.ErrNoKey:
			return err
		case rpc.ErrWrongGroup:
			// 重试，重新查询获取新配
		case rpc.ErrVersion:
			if uncertain {
				//  有此前的不确定尝试：版本可能是别人改的，也可能是自己此前写成功造成的，只能返回 ErrMaybe
				return rpc.ErrMaybe
			}
			return rpc.ErrVersion
		case rpc.ErrMaybe:
			// 下层产生了不确定性（例如请求成功执行，但是响应丢失，rpc失败）
			if !uncertain {
				uncertain = true
			}
		default:
			continue
		}
	}
}
