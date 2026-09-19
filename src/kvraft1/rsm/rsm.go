package rsm

import (
	"crypto/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// raft的日志存储的command都是以这个结构体为单位的
// raft提交的applyMsg中的command也是这个结构体
// 实际上kvserver用submit提交的req就是这里的Op.Command
type Op struct {
	// RPC 传输字段必须以大写字母开头，否则无法正确序列化。
	Command  any
	ClientID int
	RandID
}

// 上层服务（例如 ../server.go）通过 MakeRSM 构建复制状态机，并实现
// StateMachine 接口。RSM 调用 DoOp 执行业务操作，通过 Snapshot/Restore
// 保存和恢复上层状态。
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

// RSM 表示建立在 Raft 之上的复制状态机。
type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // Raft 持久化状态达到该阈值时创建快照
	sm           StateMachine
	// dist 将 Raft 日志索引映射到等待该命令结果的 Submit 调用。
	dist map[int]chan ReaderReply
}

type ReaderReply struct {
	Op
	ServerReply  any
	CommandIndex int
}

// servers[] 包含组成容错键值服务的所有 Raft 节点端点。
//
// me 是当前服务器在 servers[] 中的索引。
//
// 键值服务通过底层 Raft 保存快照；Raft 使用
// persister.SaveStateAndSnapshot() 原子保存自身状态与快照。
// 当持久化的 Raft 状态超过 maxraftstate 时，RSM 应创建快照以便裁剪日志；
// maxraftstate 为 -1 时无需创建快照。
//
// MakeRSM() 必须快速返回，长期运行的任务应放入 goroutine。
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		dist:         make(map[int]chan ReaderReply),
	}

	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
		// 重启之后，需要从persister读取快照
		if persister.SnapshotSize() > 0 {
			rsm.sm.Restore(persister.ReadSnapshot())
		}

	}

	go rsm.reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit 将命令提交给 Raft 并等待其提交。若当前节点不再是 leader，
// 返回 ErrWrongLeader，通知客户端寻找新的 leader 后重试。
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// 将业务请求封装为带唯一标识的 Op，再交给 Raft 复制。

	// 构造并提交本次操作。
	rsm.mu.Lock()

	op := Op{
		Command:  req,
		RandID:   newRandID(),
		ClientID: rsm.me,
	}

	// Start在raft中，不太耗时，可以在锁内进行
	// （把log添加到leader的日志中，持久化后就返回，replicate操作是goroutine完成的）
	// 这样可以保证键值对先插入dist，listener再读取到对应channel
	index, term, isLeader := rsm.rf.Start(op)

	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	// 在发送之前就创建channel
	// 用index（该命令提交后所在的日志索引）来映射接受回复的channel
	// leader是否改变，收到reply再判断
	ch := make(chan ReaderReply, 1)
	rsm.dist[index] = ch
	// 在结束之前释放map的内存资源
	defer rsm.deleteCh(index, ch)
	rsm.mu.Unlock()

	var reply ReaderReply

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		shouldBreak := false
		select {
		case reply = <-ch:
			shouldBreak = true
		case <-ticker.C:
			// 每隔一小段时间，检查当前的term/role状态是否已经改变
			currentTerm, isLeader := rsm.rf.GetState()
			// 状态已经改变
			// 但是命令有可能已经提交
			if currentTerm != term || !isLeader {
				return rpc.ErrWrongLeader, nil
			}
		}
		if shouldBreak {
			break
		}
	}

	// 判断当前响应是否与请求匹配，只需比较唯一id即可
	if reply.RandID != op.RandID {
		return rpc.ErrWrongLeader, nil
	}

	return rpc.OK, reply.ServerReply
}

// reader 线程，监听raft层是否提交新的applyMsg
func (rsm *RSM) reader() {
	for applyMsg := range rsm.applyCh {
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)

			// DoOp 到这里才请求kv存储服务，执行raft共识提交的指令
			serverReply := rsm.sm.DoOp(op.Command)

			if rsm.maxraftstate != -1 {
				// PersistBytes() 即raft的persister中保存的raft state的长度
				// raft 状态太大了，执行快照替换
				if rsm.rf.PersistBytes() >= rsm.maxraftstate/10*9 {
					snapshot := rsm.sm.Snapshot()
					// 这里rf替换快照是内存操作，不太耗时，如果用goroutine的话可能乱序
					rsm.rf.Snapshot(applyMsg.CommandIndex, snapshot)
				}
			}
			// 这里的mu主要保护dist，不需要负责snapshot操作
			rsm.mu.Lock()
			ch, ok := rsm.dist[applyMsg.CommandIndex]
			if !ok {
				rsm.mu.Unlock()
				continue
			}

			reply := ReaderReply{
				Op:           op,
				ServerReply:  serverReply,
				CommandIndex: applyMsg.CommandIndex,
			}

			rsm.mu.Unlock()

			ch <- reply
		} else if applyMsg.SnapshotValid {
			// 让kv存储raft install snapshot的时候提交的快照数据
			rsm.mu.Lock()
			rsm.sm.Restore(applyMsg.Snapshot)
			rsm.mu.Unlock()
		}
	}
}

type RandID [16]byte

func newRandID() RandID {
	var id RandID
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return id
}

// 结束之后需要从map中删除失效的ch，释放map的内存
func (rsm *RSM) deleteCh(index int, ch chan ReaderReply) {
	rsm.mu.Lock()
	current, ok := rsm.dist[index]
	if ok && current == ch {
		delete(rsm.dist, index)
	}
	rsm.mu.Unlock()
}
