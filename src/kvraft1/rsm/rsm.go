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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command  any
	ClientID int
	RandID
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

// replicated state machine
type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	dist map[int]chan ReaderReply
}

type ReaderReply struct {
	Op
	OpReply      any
	CommandIndex int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
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

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()

	op := Op{
		Command:  req,
		RandID:   newRandID(),
		ClientID: rsm.me,
	}
	// 在发送之前就创建channel

	// 这里Start全在锁内进行，Start函数不太耗时
	// 这样可以保证键值对先插入dist，listener再读取到对应channel
	index, term, isLeader := rsm.rf.Start(op)

	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	// 用index来映射channel，leader是否改变，收到reply再判断
	ch := make(chan ReaderReply, 1)
	rsm.dist[index] = ch

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
			if currentTerm != term || !isLeader {
				rsm.deleteCh(index, ch)
				return rpc.ErrWrongLeader, nil
			}
		}
		if shouldBreak {
			break
		}
	}

	// 只需要比较唯一id即可
	if reply.RandID != op.RandID {
		rsm.deleteCh(index, ch)
		return rpc.ErrWrongLeader, nil
	}

	rsm.deleteCh(index, ch)

	return rpc.OK, reply.OpReply
}

func (rsm *RSM) reader() {
	for applyMsg := range rsm.applyCh {
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			// DoOp的时候才真正把command应用到kv服务
			opReply := rsm.sm.DoOp(op.Command)

			if rsm.maxraftstate != -1 {
				// PersistBytes() 是raft的persister中保存的raft state的长度
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
				OpReply:      opReply,
				CommandIndex: applyMsg.CommandIndex,
			}

			rsm.mu.Unlock()

			ch <- reply
		} else if applyMsg.SnapshotValid {
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
