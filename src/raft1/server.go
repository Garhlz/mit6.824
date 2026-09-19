package raft

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"

)

const (
	SnapShotInterval = 10
)

// 服务端与测试程序之间的接口；二者分别运行在独立进程中。
type Itester interface {
	CheckLogs(int, raftapi.ApplyMsg) (string, bool)
	IngestLog(int, map[int]any)
	ApplyErr(int, string)
}

type rfsrv struct {
	ts          Itester
	me          int
	lastApplied int
	persister   *tester.Persister

	mu   sync.Mutex
	raft raftapi.Raft
	log  map[int]any // 用于校验快照恢复后的日志
}

func NewRfsrv(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	// tc 是用于连接测试程序的客户端。
	ts := newTesterProxy(tc)
	s := newRfsrv(ts, ends, grp, srv, persister, tester.MaxRaftState > 0)
	return []any{s.raft, s}
}

// 每个 Raft 服务端通过 Raft 库的 Start 提交命令，并从 applyCh 读取已提交结果。
// 测试可分别在启用或不启用快照的模式下运行服务端。
func newRfsrv(ts Itester, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister, snapshot bool) *rfsrv {

	// 在调用 raft.Make() 前复制初始快照，避免与其启动的持久化线程发生竞争。
	sn := persister.ReadSnapshot()

	s := &rfsrv{
		ts:        ts,
		me:        srv,
		log:       map[int]any{},
		persister: persister,
	}
	applyCh := make(chan raftapi.ApplyMsg)
	if !tester.UseRaftStateMachine {
		s.raft = Make(ends, srv, persister, applyCh)
	}
	if snapshot {
		if sn != nil && len(sn) > 0 {
			// 模拟 KV 服务立即处理快照；理想情况下应由 Raft 通过 applyCh 交付。
			err := s.ingestSnap(sn, -1)
			if err != "" {
				ts.ApplyErr(srv, err)
				log.Fatalf("ingestSnap err %v", err)
			}
			ts.IngestLog(s.me, s.log)
		}
		go s.applierSnap(applyCh)
	} else {
		go s.applier(applyCh)
	}
	return s
}

func (rs *rfsrv) Start(command interface{}) (int, int, bool) {
	rf := rs.getraft()
	if rf == nil {
		return 0, 0, false
	}
	return rf.Start(command)
}

func (rs *rfsrv) GetState() (int, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.raft.GetState()
}

func (rs *rfsrv) getraft() raftapi.Raft {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.raft
}

// Raft 服务端通过 CheckLogs RPC 将每条命令发送给测试程序，
// 以便测试程序核对实际收到的日志与预期是否一致。
func (rs *rfsrv) applier(applyCh chan raftapi.ApplyMsg) {
	for m := range applyCh {
		if m.CommandValid == false {
			// 忽略其他类型的 ApplyMsg。
		} else {
			err_msg, prevok := rs.ts.CheckLogs(rs.me, m)
			if m.CommandIndex > 1 && prevok == false {
				err_msg = fmt.Sprintf("server %v apply out of order %v", rs.me, m.CommandIndex)
			}
			if err_msg != "" {
				rs.ts.ApplyErr(rs.me, err_msg)
				// 即使发生错误也继续读取，避免 Raft 持锁阻塞。
			}
		}
	}
}

// 定期为 Raft 状态创建快照；从 applyCh 收到快照后，
// 通过 IngestLog RPC 将其交给测试程序。
func (rs *rfsrv) applierSnap(applyCh chan raftapi.ApplyMsg) {
	if rs.raft == nil {
		return // ???
	}

	for m := range applyCh {
		err_msg := ""
		if m.SnapshotValid {
			err_msg = rs.ingestSnap(m.Snapshot, m.SnapshotIndex)
			rs.ts.IngestLog(rs.me, rs.log)
		} else if m.CommandValid {
			if m.CommandIndex != rs.lastApplied+1 {
				err_msg = fmt.Sprintf("server %v apply out of order, expected index %v, got %v", rs.me, rs.lastApplied+1, m.CommandIndex)
			}

			if err_msg == "" {
				var prevok bool
				err_msg, prevok = rs.ts.CheckLogs(rs.me, m)
				if err_msg != "ErrRPC" && m.CommandIndex > 1 && prevok == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", rs.me, m.CommandIndex)
				}
			}

			rs.log[m.CommandIndex] = m.Command // 保存日志以生成快照
			rs.lastApplied = m.CommandIndex

			if (m.CommandIndex+1)%SnapShotInterval == 0 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(m.CommandIndex)
				var xlog []any
				for j := 0; j <= m.CommandIndex; j++ {
					xlog = append(xlog, rs.log[j])
				}
				e.Encode(xlog)
				start := tester.GetAnnotatorTimestamp()
				rf := rs.getraft()
				rf.Snapshot(m.CommandIndex, w.Bytes())
				desp := fmt.Sprintf("snapshot created by %v", rs.me)
				details := fmt.Sprintf(
					"snapshot created by server %v after applying the command at index %v",
					rs.me,
					m.CommandIndex)
				tester.PostAnnotatorInfoInterval(start, desp, details)
			}
		} else {
			// 忽略其他类型的 ApplyMsg。
		}
		if err_msg != "" {
			rs.ts.ApplyErr(rs.me, err_msg)
			// 即使发生错误也继续读取，避免 Raft 持锁阻塞。
		}
	}
}

// 成功时返回空字符串，失败时返回错误描述。
func (rs *rfsrv) ingestSnap(snapshot []byte, index int) string {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if snapshot == nil {
		return "nil snapshot"
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var xlog []any
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&xlog) != nil {
		return "failed to decode snapshot"
	}
	if index != -1 && index != lastIncludedIndex {
		err := fmt.Sprintf("server %v snapshot doesn't match m.SnapshotIndex", rs.me)
		return err
	}
	rs.log = map[int]any{}
	for j := 0; j < len(xlog); j++ {
		rs.log[j] = xlog[j]
	}
	rs.lastApplied = lastIncludedIndex
	return ""
}

type GetStateArgs struct{}

type GetStateReply struct {
	Term   int
	Leader bool
}

func (rs *rfsrv) GetStateRPC(args *GetStateArgs, rep *GetStateReply) {
	rep.Term, rep.Leader = rs.GetState()
}

type StartArgs struct {
	Command any
}

type StartReply struct {
	Index  int
	Term   int
	Leader bool
}

func (rs *rfsrv) StartRPC(args *StartArgs, rep *StartReply) {
	rep.Index, rep.Term, rep.Leader = rs.Start(args.Command)
}
