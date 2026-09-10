package raft

// ../raftapi/raftapi.go 定义了 Raft 必须向上层服务（或测试程序）
// 暴露的接口；各函数的具体要求见下方注释。
//
// Make() 用于创建一个实现 raft 接口的新 Raft 节点。

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

var heartbeatInterval int = 150

// Raft 表示一个 Raft 节点的 Go 实现。
type Raft struct {
	mu              sync.Mutex          // 保护当前节点共享状态的互斥锁
	peers           []*labrpc.ClientEnd // 所有 Raft 节点的 RPC 端点
	persister       *tester.Persister   // 保存当前节点持久化状态的对象
	snapshot        []byte
	pendingSnapshot *raftapi.ApplyMsg

	me int // 当前节点在 peers[] 中的索引
	// 在此添加实验 3A、3B、3C 所需的状态。
	// Raft 节点应维护的状态详见论文图 2。
	role        Role // 当前节点的角色
	currentTerm int
	votedFor    int

	logEntries        []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int

	commitIndex int // 已经被多数节点确认提交的最高索引
	lastApplied int // 已经通过 applyCh 交给状态机的最高索引

	nextIndex  []int
	matchIndex []int

	electionDeadline  time.Time
	heartbeatDeadline time.Time

	applyCh chan raftapi.ApplyMsg
	// replicateChs []chan struct{}
	applyCond *sync.Cond
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// GetState 返回当前任期，以及当前节点是否认为自己是 leader。
// 会被其他程序并发调用，必须加锁
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// 在此实现实验 3A 所需逻辑。
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// persist 将 Raft 的持久化状态保存到稳定存储，以便节点崩溃重启后恢复。
// 需要持久化的状态见论文图 2。实现快照前，应向 persister.Save()
// 的第二个参数传入 nil；实现快照后，则传入当前快照（尚无快照时仍传 nil）。
// 调用者加锁
func (rf *Raft) persist() {
	// 在此实现实验 3C 所需逻辑。

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// readPersist 恢复此前持久化的 Raft 状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil { // 没有可恢复状态时按全新节点启动
		return
	}
	if len(data) < 1 {
		return
	}

	// 在此实现实验 3C 所需逻辑。
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		panic("read persist error")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntries = append([]LogEntry(nil), log...)
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.mu.Unlock()
	}

}

// PersistBytes 返回 Raft 持久化状态占用的字节数。
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Snapshot 表示上层服务已创建一个包含 index 及其之前全部状态的快照。
// 因此，上层服务不再需要 index 及其之前的日志，Raft 应尽可能裁剪这些日志。
// client调用这个rpc，要求raft集群把小于等于index的log删掉
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 在此实现实验 3D 所需逻辑。
	rf.mu.Lock()
	if index <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	// if index-rf.lastIncludedIndex >= len(rf.logEntries) {
	// 	panic("index too big")
	// }
	rf.logEntries = append([]LogEntry{}, rf.logEntries[index-rf.lastIncludedIndex:]...)
	rf.logEntries[0].Command = nil
	// 第0项恰好就是last include index, 现在作为哨兵
	rf.snapshot = append([]byte{}, snapshot...)
	rf.lastIncludedIndex = index
	term := rf.logEntries[0].Term
	rf.lastIncludedTerm = term
	rf.persist()
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// todo 先只实现心跳功能的部分
	// 权衡之后，目前情况全局加锁即可
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 该rpc仅用于心跳的时候同样走下面的逻辑
	// 旧的请求，直接打回
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 只有请求的term大于当前term才会重置投票结果
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	rf.role = Follower
	rf.updateElectionDeadline()
	localPrevLogIndex := args.PrevLogIndex - rf.lastIncludedIndex
	// 数组越界
	if localPrevLogIndex >= len(rf.logEntries) {
		reply.Success = false
		reply.Term = rf.currentTerm
		// 表示当前peer的log太短，直接从conflict index之后发过来
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.logEntries) + rf.lastIncludedIndex
		return
	}

	// 告诉leader要从当前peer的LII之后开始发
	if localPrevLogIndex < 0 {
		reply.Success = false
		reply.Term = rf.currentTerm
		// 表示当前peer的log太短，直接从conflict index之后发过来
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}

	// 如果prevlogterm匹配
	if rf.logEntries[localPrevLogIndex].Term == args.PrevLogTerm {
		for i := range args.Entries {
			localIndex := localPrevLogIndex + 1 + i
			// 越界，直接添加
			if localIndex >= len(rf.logEntries) {
				rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
				rf.persist()
				break
			}
			// 遇到第一个不匹配的，后面的直接截断添加
			if rf.logEntries[localIndex].Term != args.Entries[i].Term {
				rf.logEntries = append(rf.logEntries[:localIndex], args.Entries[i:]...)
				rf.persist()
				break
			}
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
			rf.applyCond.Signal()
		}

		reply.Success = true
		reply.Term = rf.currentTerm

	} else {
		// 找到peer本地不匹配的最早的一个term
		conflictTerm := rf.logEntries[localPrevLogIndex].Term
		conflictIndex := localPrevLogIndex

		for conflictIndex > 0 && rf.logEntries[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex + rf.lastIncludedIndex
		reply.ConflictTerm = conflictTerm
		// leader需要判断自己是否有这个conflictTerm
		reply.Success = false
		reply.Term = rf.currentTerm
	}
}

// RequestVoteArgs 定义 RequestVote RPC 的请求参数。
// 通过 RPC 传输的字段名必须以大写字母开头。
type RequestVoteArgs struct {
	// 在此添加实验 3A、3B 所需字段。
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply 定义 RequestVote RPC 的响应参数。
// 通过 RPC 传输的字段名必须以大写字母开头。
type RequestVoteReply struct {
	// 在此添加实验 3A 所需字段。
	Term        int
	VoteGranted bool
}

// RequestVote 处理其他节点发来的投票请求。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 在此实现实验 3A、3B 所需逻辑。
	// 权衡之后发现，直接全局加锁即可
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		// 这里如果原本就是follower，是否应该设置votedFor = -1?
		// 如果请求的term更大确实应该重置投票对象
		rf.becomeFollower(args.Term)
		rf.persist()
		// 只是收到更高term的请求，不重置 election deadline
		// rf.updateElectionDeadline()
	}
	// candidate的term和当前term相同，当前server不一定会直接变成Follower
	lastLogIndex := len(rf.logEntries) - 1
	lastLogTerm := rf.logEntries[lastLogIndex].Term

	// 如果当前server的日志比candidate还要新，则拒绝投票
	refuseVote := false
	if lastLogTerm > args.LastLogTerm {
		refuseVote = true
	} else if lastLogTerm == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex {
		// 这里换成绝对索引
		refuseVote = true
	}

	if refuseVote {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		// 更新投票对象
		rf.votedFor = args.CandidateID
		rf.persist()
		// 向别人投票之后，自己的选举计时器也需要更新
		rf.updateElectionDeadline()
		return
	}
	// 应该是当前follower的leader任期还未结束
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.updateElectionDeadline()
	}
	rf.role = Follower
	rf.updateElectionDeadline()

	// 依然是并发中的旧信息问题
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex < rf.lastLogIndex() &&
		rf.logEntries[args.LastIncludedIndex-rf.lastIncludedIndex].Term == args.LastIncludedTerm {
		rf.logEntries = append([]LogEntry{}, rf.logEntries[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
		rf.logEntries[0].Command = nil
	} else {
		rf.logEntries = append([]LogEntry{}, LogEntry{
			Command: nil, Term: args.LastIncludedTerm,
		})
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = append([]byte{}, args.Snapshot...)

	// 上层应用发来的快照，证明提交信息至少有这么多
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.persist()

	// 通知applier快照已发生改变，需要发给上层应用
	// 只允许更大的快照覆盖
	if rf.pendingSnapshot == nil || rf.lastIncludedIndex > rf.pendingSnapshot.SnapshotIndex {
		rf.pendingSnapshot = &raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      append([]byte(nil), args.Snapshot...),
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
		rf.applyCond.Signal()
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()

}

// 以下代码用于向其他节点发送 RequestVote RPC。
// server 是目标节点在 rf.peers[] 中的索引，args 是 RPC 请求参数。
// RPC 响应会写入 *reply，因此调用方应传入 &reply。
// 传给 Call() 的 args 和 reply 类型必须与 RPC handler 声明的参数类型一致，
// 包括参数是否为指针。
//
// labrpc 会模拟可能丢包的网络：节点可能不可达，请求或响应也可能丢失。
// Call() 发送请求并等待响应；若在超时时间内收到响应则返回 true，
// 否则返回 false，因此 Call() 可能需要一段时间才会返回。
// 返回 false 可能表示目标节点宕机、节点不可达、请求丢失或响应丢失。
//
// 除非服务端 handler 始终不返回，否则 Call() 最终一定会返回，
// 因此无需在 Call() 外额外实现超时机制。
//
// 更多细节参见 ../labrpc/labrpc.go 中的注释。
//
// 若 RPC 无法正常工作，请检查传输结构体的字段名是否均以大写字母开头，
// 并确认调用方传入的是响应结构体的地址 &reply，而不是结构体本身。

func (rf *Raft) startElection() {

	voteReceived := 1

	rf.mu.Lock()
	// 在锁内构造参数的快照
	electionTerm := rf.currentTerm
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.logEntries[len(rf.logEntries)-1].Term
	rf.mu.Unlock()

	type Result struct {
		ok       bool
		serverID int
		reply    RequestVoteReply
	}
	results := make(chan Result, len(rf.peers)-1)

	for serverID := range len(rf.peers) {
		if serverID == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         electionTerm,
			CandidateID:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := RequestVoteReply{}
		// 用goroutine执行耗时的 rpc 请求
		go func(serverID int, args *RequestVoteArgs, reply *RequestVoteReply) {
			// 向这个服务器发送投票申请
			ok := rf.peers[serverID].Call("Raft.RequestVote", args, reply)
			results <- Result{ok: ok, serverID: serverID, reply: *reply}
		}(serverID, &args, &reply)
	}

	for range len(rf.peers) - 1 {
		result := <-results

		if result.ok {
			rf.mu.Lock()
			// 接收到了更新的 term，转而成为follower
			if result.reply.Term > rf.currentTerm {
				rf.becomeFollower(result.reply.Term)
				rf.persist()
				// rf.updateElectionDeadline()
				rf.mu.Unlock()
				return
			}

			// 整轮选举已经过期，后续回复也没有意义
			if rf.currentTerm != electionTerm || rf.role != Candidate {
				rf.mu.Unlock()
				return
			}

			if result.reply.VoteGranted {
				voteReceived += 1
			}

			// 选举中间已经获得了过半票，直接成为leader，直接结束当前阻塞的等待
			if voteReceived > len(rf.peers)/2 {
				rf.becomeLeader()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}

	total := len(rf.peers)
	// 选举成功，成为leader
	if voteReceived > total/2 {
		rf.mu.Lock()
		// 可能在rpc过程中已经有别人成为candidate了
		if rf.currentTerm != electionTerm || rf.role != Candidate {
			rf.mu.Unlock()
			// 整轮选举已经过期，后续回复也没有意义
			return
		}
		rf.becomeLeader()
		rf.mu.Unlock()
	}
	// 选举失败
	// 超时并通过增加其任期，启动另一轮请求投票RPC
}

func (rf *Raft) sendHeartbeat() {

	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	// 都要加锁！
	term := rf.currentTerm
	rf.mu.Unlock()
	for peer := range len(rf.peers) {
		if peer == rf.me {
			continue
		}

		go rf.replicateToPeer(peer, term)
	}

	rf.mu.Lock()
	rf.heartbeatDeadline = time.Now().Add(time.Duration(heartbeatInterval) * time.Millisecond)
	rf.mu.Unlock()
}

func (rf *Raft) ticker() {

	for {
		// 在此实现实验 3A 所需逻辑，检查是否应发起 leader 选举。
		time.Sleep(5 * time.Millisecond)

		rf.mu.Lock()

		needElection, needSendHeartbeat := false, false
		if rf.role == Leader {
			if time.Now().After(rf.heartbeatDeadline) {
				needSendHeartbeat = true
			}
		} else {
			if time.Now().After(rf.electionDeadline) {
				rf.becomeCandidate()
				rf.persist()
				needElection = true

			}
		}

		rf.mu.Unlock()

		if needElection {
			// 有rpc的耗时操作用goroutine进行
			go rf.startElection()
		}
		if needSendHeartbeat {
			rf.sendHeartbeat()
		}
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	for {
		for rf.lastApplied >= rf.commitIndex && rf.pendingSnapshot == nil {
			// 1. 释放锁，阻塞当前goroutine
			// 2. 等待被唤醒之后，获取锁，然后返回
			rf.applyCond.Wait()
		}
		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil

			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()

			rf.lastApplied = max(rf.lastApplied, msg.SnapshotIndex)
			continue
		}
		next := rf.lastApplied + 1
		entry := rf.logEntries[next-rf.lastIncludedIndex]
		rf.lastApplied = next
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: next,
		}
		rf.mu.Unlock()
		// 锁外执行耗时的发送操作
		rf.applyCh <- msg
		rf.mu.Lock()
	}
}

func (rf *Raft) replicateToPeer(peer int, leaderTerm int) {
	for {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}

		if rf.nextIndex[peer] <= rf.lastIncludedIndex {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderID:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Snapshot:          append([]byte{}, rf.snapshot...),
			}
			reply := InstallSnapshotReply{}
			rf.mu.Unlock()

			ok := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)

			if !ok {
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				// term已更新，转为follower
				rf.becomeFollower(reply.Term)
				rf.persist()
				// rf.updateElectionDeadline()
				rf.mu.Unlock()
				return
			}

			if rf.role != Leader || rf.currentTerm != leaderTerm {
				rf.mu.Unlock()
				return
			}

			rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
			rf.nextIndex[peer] = max(rf.nextIndex[peer], args.LastIncludedIndex+1)
			// 安装完之后下一轮再发送
			rf.mu.Unlock()
			continue
		}
		// leader 在其日志中包含紧邻新条目之前的条目的index和term
		// 计算的时候都用相对，存储和传递的时候用绝对的索引
		nextIndex := rf.nextIndex[peer] - rf.lastIncludedIndex
		prevIndex := nextIndex - 1
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevIndex + rf.lastIncludedIndex,
			PrevLogTerm:  rf.logEntries[prevIndex].Term,
			Entries:      append([]LogEntry{}, rf.logEntries[nextIndex:]...), // 注意构造 RPC 快照时应复制
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		matched := prevIndex + len(args.Entries) + rf.lastIncludedIndex
		requestNextIndex := rf.nextIndex[peer]
		rf.mu.Unlock()

		// 锁外调用耗时的rpc
		ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			// term已更新，转为follower
			rf.becomeFollower(reply.Term)
			rf.persist()
			// rf.updateElectionDeadline()
			rf.mu.Unlock()
			return
		}

		if rf.role != Leader || rf.currentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// 根据args的参数计算当前匹配的长度。旧的成功回复可能会把index倒退，这里保持递增
			rf.matchIndex[peer] = max(rf.matchIndex[peer], matched)
			rf.nextIndex[peer] = max(rf.nextIndex[peer], matched+1)

			// 如果存在N满足N > commitIndex，使得多数matchIndex[i] ≥N 且 log[N].term == currentTerm，setcommitIndex = N
			hasNewCommit, newCommitIndex := rf.checkCommitIndex()
			if hasNewCommit {
				// 这里只负责更新commitIndex，发送applymsg的事情由applier处理
				rf.commitIndex = newCommitIndex
				rf.applyCond.Signal()
			}
			// 如果当前peer的日志还没有追上leader，继续循环
			if rf.nextIndex[peer] < rf.lastIncludedIndex+len(rf.logEntries) {
				rf.mu.Unlock()
				// continue
				return
			}
			rf.mu.Unlock()
			return
		} else {
			// 已经有另一个rpc修改了进度，当前响应已经过期
			if rf.nextIndex[peer] != requestNextIndex {
				rf.mu.Unlock()
				return
			}
			// 之前的 prev index 在peer中越界了
			if reply.ConflictTerm == -1 {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				// 查看leader自己是否有这个conflict term
				hasTerm := false
				for i := len(rf.logEntries) - 1; i >= 0; i-- {
					if rf.logEntries[i].Term == reply.ConflictTerm {
						hasTerm = true
						rf.nextIndex[peer] = i + 1 + rf.lastIncludedIndex
						break
					}
				}
				if !hasTerm {
					rf.nextIndex[peer] = reply.ConflictIndex
				}

			}

			rf.mu.Unlock()
			return
		}
	}
}

// 调用者加锁
func (rf *Raft) becomeLeader() {
	rf.role = Leader
	// 只有成为leader之后才会启用心跳时间
	rf.heartbeatDeadline = time.Now().Add(time.Duration(heartbeatInterval) * time.Millisecond)
	// 成为leader之后，把所有follower的nextIndex更新为领导者的最后日志索引 + 1
	// matchIndex更新为 0
	for i := range len(rf.nextIndex) {
		// 都用绝对索引
		rf.nextIndex[i] = len(rf.logEntries) + rf.lastIncludedIndex
		rf.matchIndex[i] = rf.lastIncludedIndex
	}
}

// 调用者加锁
func (rf *Raft) becomeFollower(newTerm int) {
	rf.role = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

// 调用者加锁
func (rf *Raft) becomeCandidate() {
	rf.role = Candidate
	rf.currentTerm += 1
	// 投票给自己
	rf.votedFor = rf.me
	rf.updateElectionDeadline()
}

// 调用者加锁
func (rf *Raft) updateElectionDeadline() {
	ms := time.Duration(300 + rand.Int63()%200)
	rf.electionDeadline = time.Now().Add(ms * time.Millisecond)
}

// Make 创建一个 Raft 节点。peers[] 包含集群中所有节点（包括当前节点）的
// RPC 端点，当前节点对应 peers[me]；所有节点看到的 peers[] 顺序一致。
// persister 用于保存当前节点的持久化状态，并在启动时持有最近一次保存的状态。
// Raft 应通过 applyCh 向测试程序或上层服务发送 ApplyMsg。
// Make() 必须快速返回，所有长期运行的任务都应放入 goroutine 中执行。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// 在此完成实验 3A、3B、3C 所需的初始化。
	rf.mu.Lock()
	rf.becomeFollower(0)
	rf.updateElectionDeadline()
	rf.logEntries = []LogEntry{}
	// 引入哨兵日志
	rf.logEntries = append(rf.logEntries, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.mu.Unlock()

	// 恢复节点崩溃前持久化的状态。
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = append([]byte{}, rf.persister.ReadSnapshot()...)
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	// 启动 ticker goroutine，负责触发选举。
	go rf.ticker()
	go rf.applier()

	return rf
}

// Start 由使用 Raft 的上层服务（例如键值服务）调用，用于开始对一条待追加到
// Raft 日志的新命令达成一致。如果当前节点不是 leader，则返回 false；
// 否则启动一致性过程并立即返回。由于 leader 可能宕机或选举失败，
// 该命令不保证最终一定会提交到 Raft 日志。
//
// 第一个返回值是该命令提交后所在的日志索引，第二个返回值是当前任期，
// 第三个返回值表示当前节点是否认为自己是 leader。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return rf.lastLogIndex(), rf.currentTerm, false
	} else {
		term := rf.currentTerm
		// 这里只需要负责更新logEntries即可，worker会根据它进行发送
		rf.logEntries = append(rf.logEntries, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.persist()
		// 这里是避免只有一个节点的情况下不会更新commitIndex,从而不会更新apply
		hasNewCommit, newCommitIndex := rf.checkCommitIndex()
		if hasNewCommit {
			// 这里只负责更新commitIndex，发送applymsg的事情由applier处理
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}

		for peer := range len(rf.peers) {
			// 加锁构造 follower 的参数快照
			if peer == rf.me {
				continue
			}
			go rf.replicateToPeer(peer, rf.currentTerm)
		}
		// 这里返回的是绝对索引
		return rf.lastLogIndex(), term, true
	}
}

// 持锁时调用
// 该函数中所有索引都是绝对的
func (rf *Raft) checkCommitIndex() (bool, int) {
	for N := rf.lastLogIndex(); N > rf.commitIndex; N-- {
		if rf.logEntries[N-rf.lastIncludedIndex].Term != rf.currentTerm {
			continue
		}
		cnt := 1
		for serverID := range len(rf.peers) {
			if serverID == rf.me {
				continue
			}
			if rf.matchIndex[serverID] >= N {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			return true, N
		}
	}
	return false, rf.commitIndex
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.logEntries) - 1
}
