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
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck        kvtest.IKVClerk
	lockKey   string
	lockValue string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lockValue := "lock-" + randID()
	lk := &Lock{
		ck:        ck,
		lockKey:   lockname,
		lockValue: lockValue}

	// You may add code here
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
	// Your code here
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
	// Your code here
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
