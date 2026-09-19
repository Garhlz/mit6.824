package lock

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk 是键值服务 Clerk 的 Go 接口：它隐藏具体实现类型，
	// 但保证提供 Put 和 Get。测试程序调用 MakeLock() 时会传入该 Clerk。
	ck        kvtest.IKVClerk
	lockKey   string
	lockValue string
	// 可在此补充分布式锁所需的客户端状态。
}

// 测试程序调用 MakeLock() 并传入键值服务 Clerk；锁可通过
// lk.ck.Put() 和 lk.ck.Get() 访问共享状态。
//
// lockname 用于区分多把锁；名称不同的锁应彼此独立。
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lockValue := "lock-" + randID()
	lk := &Lock{
		ck:        ck,
		lockKey:   lockname,
		lockValue: lockValue}

	// 可在此完成锁状态的初始化。
	err := ck.Put(lockname, "", 0)
	/*
		OK          自己创建了锁 key
		ErrVersion  其他客户端已经创建
		ErrMaybe    某个 version 0 的创建已经发生，但无法确定是谁创建的
	*/
	if err != rpc.OK && err != rpc.ErrVersion && err != rpc.ErrMaybe {
		panic(fmt.Sprintf("initialize lock %q: unexpected error %v", lockname, err))
	}
	return lk
}

func (lk *Lock) Acquire() {
	// 循环读取并尝试占有锁，直到确认获取成功。
	for {
		value, version, err := lk.ck.Get(lk.lockKey)
		if err != rpc.OK {
			panic(fmt.Sprintf("acquire lock %q: get state: %v", lk.lockKey, err))
		}

		// 有人获取了锁，重试
		if value != "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		switch err = lk.ck.Put(lk.lockKey, lk.lockValue, version); err {
		case rpc.OK:
			return
		case rpc.ErrVersion:
			// 其他客户端抢先修改了锁，重新读取最新状态。
			time.Sleep(100 * time.Millisecond)
			continue
		case rpc.ErrMaybe:
			value1, _, err1 := lk.ck.Get(lk.lockKey)
			if err1 != rpc.OK {
				panic(fmt.Sprintf("acquire lock %q: confirm uncertain put: %v", lk.lockKey, err1))
			}
			// 之前已经put成功，但是因为网络问题没有收到回复
			if value1 == lk.lockValue {
				return
			}
			// 否则自己没有持有锁，重新读取并竞争。
			time.Sleep(100 * time.Millisecond)
			continue
		default:
			panic(fmt.Sprintf("acquire lock %q: unexpected put error %v", lk.lockKey, err))
		}
	}
}

func (lk *Lock) Release() {
	// 仅允许锁的当前持有者释放锁。
	for {
		value, version, err := lk.ck.Get(lk.lockKey)
		if err != rpc.OK {
			panic(fmt.Sprintf("release lock %q: get state: %v", lk.lockKey, err))
		}

		if value != lk.lockValue {
			// 可能上次循环put "" 成功了，但是返回网络错误
			// 甚至其他进程已经在释放之后获取了锁，所以value不是自己的
			return
		}

		switch err = lk.ck.Put(lk.lockKey, "", version); err {
		case rpc.OK:
			return
		case rpc.ErrVersion, rpc.ErrMaybe:
			// 状态已改变或结果不确定，重新读取后再决定是否需要释放。
			time.Sleep(100 * time.Millisecond)
			continue
		default:
			panic(fmt.Sprintf("release lock %q: unexpected put error %v", lk.lockKey, err))
		}
	}
}

func randID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
