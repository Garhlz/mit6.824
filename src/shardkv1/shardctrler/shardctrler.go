package shardctrler

//
// shardctrler 提供 InitConfig、Query 和 ChangeConfigTo，用于管理配置迁移。
//

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler 同时持有 controller 的通信端点与配置存储 Clerk。
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
	killed int32 // 由 Kill() 设置
	// 可在此补充 controller 状态。
}

// MakeShardCtrler 创建将配置状态存储在 kvsrv 中的 controller。
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	// controller应该是group0的0号服务器
	srv := tester.ServerName(tester.GRP0, 0)
	// controller连接的存储config的kv服务
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// 在此初始化 controller 的附加状态。
	return sck
}

func (sck *ShardCtrler) getNewConfig() (*shardcfg.ShardConfig, *shardcfg.ShardConfig, rpc.Tversion) {
	for {
		currentValue, _, err := sck.Get(currentName)
		if err != rpc.OK {
			panic("get config error")
		}
		currentConfig := shardcfg.FromString(currentValue)
		nextValue, newVersion, err := sck.Get(nextName)
		if err != rpc.OK {
			panic("get config error")
		}
		nextConfig := shardcfg.FromString(nextValue)
		newCurrentValue, _, err := sck.Get(currentName)
		newCurrentConfig := shardcfg.FromString(newCurrentValue)
		if err != rpc.OK {
			panic("get config error")
		}
		if !currentConfig.EqualTo(newCurrentConfig) {
			// 配置已经更改，重新读取
			continue
		}
		if nextConfig.Num < newCurrentConfig.Num {
			// 有读取交错
			continue
		}
		return newCurrentConfig, nextConfig, newVersion
	}
}

// 测试程序在启动每个新 controller 前调用 InitController()。
// Part A 无需恢复；Part B/C 用它检查并继续未完成的配置迁移。
func (sck *ShardCtrler) InitController() {
	for {
		currentConfig, nextConfig, _ := sck.getNewConfig()
		// 不存在迁移
		if currentConfig.EqualTo(nextConfig) {
			return
		}

		// 有进行中的配置迁移
		ok := sck.doConfigChange(currentConfig, nextConfig)
		if ok {
			// 下次循环开头判断
			continue
		}

		newCurrentValue, _, err := sck.Get(currentName)
		if err != rpc.OK {
			panic("get config error")
		}
		newCurrentConfig := shardcfg.FromString(newCurrentValue)

		if newCurrentConfig.Num >= nextConfig.Num {
			// 已经成功或者更新
			return
		}

		if !newCurrentConfig.EqualTo(currentConfig) {
			// 已经被更新过了，这里退出
			return
		}

		if nextConfig.Num < currentConfig.Num {
			// 状态读取混乱，重试
			continue
		}
	}
}

const (
	currentName = "current_config"
	nextName    = "next_config"
)

// InitConfig 由测试程序调用一次，用于写入初始配置。
// 可用 shardcfg.String() 序列化配置，并以 kvsrv key 的版本 0 写入；
// 初始配置将所有 shard 分配给 shardcfg.Gid1。
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// current 与 next 初始相同，表示不存在进行中的迁移。
	v := cfg.String()
	_ = sck.Put(currentName, v, 0)
	_ = sck.Put(nextName, v, 0)
}

// ChangeConfigTo 将 current 配置迁移到 new。迁移期间当前 controller
// 可能被其他 controller 取代，因此所有步骤都必须可恢复且可重复。
func (sck *ShardCtrler) ChangeConfigTo(proposal *shardcfg.ShardConfig) {
	// 先恢复已有迁移，再通过版本化 Put 竞争下一次配置变更。
	/*
		next.Num > current.Num
		系统存在未完成的 current → next 迁移，
		当前 controller 帮忙完成它
	*/
retry:
	sck.InitController()

	currentConfig, nextConfig, nextVersion := sck.getNewConfig()

	// 当前提案已经过期
	if proposal.Num <= currentConfig.Num {
		return
	}

	// 有权进行迁移（current config的编号只能单调递增）
	if currentConfig.EqualTo(nextConfig) && proposal.Num == currentConfig.Num+1 {
		putError := sck.Put(nextName, proposal.String(), nextVersion)

		switch putError {
		case rpc.OK:
			// 成功设置了next_config
			ok := sck.doConfigChange(currentConfig, proposal)
			if !ok {
				goto retry
			}
			return
		case rpc.ErrVersion:
			// 可能有两个controller提交同编号不同内容，但是只有一个可以竞争成功更新next config
			// 本次竞争失败，可以结束
			return
		case rpc.ErrMaybe:
			currentConfig, nextConfig, _ := sck.getNewConfig()
			if proposal.EqualTo(nextConfig) {
				// 目标配置已经持久化，无法区分是不是自己写的，但可以迁移
				ok := sck.doConfigChange(currentConfig, proposal)
				if !ok {
					goto retry
				}
				return
			} else {
				// 其他进程已经获胜
				return
			}
		default:
			return
		}
	}

}

// 执行已经选定的 old → target 迁移；所有 shard 搬完后，尝试把 current 从 old 更新成 target。
// true：这个目标已完成，或系统已经越过它，无需再处理。
// false：本次未确认完成，调用方需要重新读取配置判断
func (sck *ShardCtrler) doConfigChange(old, target *shardcfg.ShardConfig) bool {
	// 这里实际上不需要传输一个task
	type Task struct {
		shardID    shardcfg.Tshid
		oldGroupID tester.Tgid
		newGroupID tester.Tgid
	}
	taskCount := 0
	tasks := []Task{}
	// 已经设定了next_config，开始迁移
	for shardID := range shardcfg.NShards {
		// 搜索需要迁移的任务
		if oldGroupID, newGroupID := old.Shards[shardID], target.Shards[shardID]; oldGroupID != newGroupID {
			taskCount++
			tasks = append(tasks, Task{
				shardID:    shardcfg.Tshid(shardID),
				oldGroupID: oldGroupID,
				newGroupID: newGroupID})
		}
	}

	resultCh := make(chan bool, taskCount)

	for _, task := range tasks {
		go func(t Task) {
			// 需要把shard i从gid1迁移到gid2
			oldGroupClerk := shardgrp.MakeClerk(sck.clnt, old.Groups[t.oldGroupID])
			shardData, err := oldGroupClerk.FreezeShard(t.shardID, target.Num)
			// 这里如果遇到就返回失败，让上层重新读取配置重试
			if err != rpc.OK {
				resultCh <- false
				return
			}
			newGroupClerk := shardgrp.MakeClerk(sck.clnt, target.Groups[t.newGroupID])
			err = newGroupClerk.InstallShard(t.shardID, shardData, target.Num)
			if err != rpc.OK {
				resultCh <- false
				return
			}
			err = oldGroupClerk.DeleteShard(t.shardID, target.Num)
			if err != rpc.OK {
				resultCh <- false
				return
			}
			resultCh <- true
		}(task)
	}

	successCount := 0
	for range taskCount {
		result := <-resultCh
		if result {
			successCount++
		}
	}

	if successCount < taskCount {
		return false
	}

	// 所有shard迁移任务全部成功，才可以把新配置发布到current
	currentConfig, _, _ := sck.getNewConfig()
	// 重新检查新的current config value

	if currentConfig.EqualTo(target) {
		// 别人已经发布迁移，返回
		return true
	}
	if currentConfig.Num > target.Num {
		// 自己的请求已经过期，旧任务结束
		return true
	}

	if currentConfig.EqualTo(old) {
		// 使用最新读取的版本，尝试发布 target
		v, currentVersion, err := sck.Get(currentName)
		if err != rpc.OK {
			panic("kv client get fail")
		}
		currentConfig = shardcfg.FromString(v)
		if !currentConfig.EqualTo(old) {
			return false
		}
		err = sck.Put(currentName, target.String(), currentVersion)
		switch err {

		case rpc.OK:
			// 发布成功
			return true
		case rpc.ErrMaybe, rpc.ErrVersion:
			currentConfig, _, _ := sck.getNewConfig()
			// 重新对比需要重新获取要对比的value
			if currentConfig.EqualTo(target) || currentConfig.Num > target.Num {
				return true
			}
			return false
		default:
			return false
		}
	}
	// 本次观察不符合预期，返回 false，让外层重新判断
	return false
}

// Query 返回已经完成迁移并正式生效的 current 配置。
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// 客户端路由只能依据 current，不能提前暴露 next。
	value, _, err := sck.Get(currentName)
	if err != rpc.OK {
		panic("query config error")
	}

	return shardcfg.FromString(value)
}
