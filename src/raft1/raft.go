package raft

// ../raftapi/raftapi.go 定义了 Raft 必须向上层服务（或测试程序）
// 暴露的接口；各函数的具体要求见下方注释。
//
// Make() 用于创建一个实现 raft 接口的新 Raft 节点。

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// Role 表示 Raft 节点当前所处的角色。
type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

// heartbeatInterval 是 leader 发送心跳的间隔，单位为毫秒。
var heartbeatInterval int = 150

// Raft 表示一个 Raft 节点的 Go 实现。
type Raft struct {
	mu              sync.Mutex          // 保护当前节点共享状态的互斥锁
	peers           []*labrpc.ClientEnd // 所有 Raft 节点的 RPC 端点
	persister       *tester.Persister   // 保存当前节点持久化状态的对象
	snapshot        []byte              // 最近一次持久化的状态机快照
	pendingSnapshot *raftapi.ApplyMsg   // 尚未交付给状态机的新快照

	me int // 当前节点在 peers[] 中的索引
	// 在此添加实验 3A、3B、3C、3D 所需的状态。
	// Raft 节点应维护的状态详见论文图 2。
	role        Role // 当前节点的角色
	currentTerm int
	votedFor    int

	logEntries        []LogEntry // 下标 0 是快照边界处的哨兵日志
	lastIncludedIndex int        // 快照包含的最后一条日志的绝对索引
	lastIncludedTerm  int        // lastIncludedIndex 对应日志的任期

	commitIndex int // 已经被多数节点确认提交的最高索引
	lastApplied int // 已经通过 applyCh 交给状态机的最高索引

	nextIndex  []int // leader 记录各 follower 下一条待发送日志的绝对索引
	matchIndex []int // leader 已确认各 follower 匹配的最高绝对索引

	electionDeadline  time.Time
	heartbeatDeadline time.Time

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
}

// LogEntry 表示一条由客户端命令及其创建任期组成的 Raft 日志。
type LogEntry struct {
	Command interface{}
	Term    int
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

	// 初始化节点状态以及快照边界处的哨兵日志。
	rf.mu.Lock()
	rf.becomeFollower(0)
	rf.updateElectionDeadline()
	rf.logEntries = []LogEntry{}
	// 哨兵日志简化 PrevLogIndex/PrevLogTerm 的边界处理。
	rf.logEntries = append(rf.logEntries, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.mu.Unlock()

	// 恢复节点崩溃前持久化的状态。
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = append([]byte{}, rf.persister.ReadSnapshot()...)
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// ticker 负责选举与心跳，applier 负责按顺序交付已提交状态。
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
		// leader 先在本地追加日志，再根据各 follower 的复制进度异步发送。
		rf.logEntries = append(rf.logEntries, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.persist()
		// 单节点集群无需等待 RPC，也可能立即满足多数派提交条件。
		hasNewCommit, newCommitIndex := rf.checkCommitIndex()
		if hasNewCommit {
			// 这里只推进 commitIndex，具体交付由 applier 统一完成。
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}

		for peer := range len(rf.peers) {
			if peer == rf.me {
				continue
			}
			go rf.replicateToPeer(peer, rf.currentTerm)
		}
		// 对外返回未经 slice 偏移换算的绝对日志索引。
		return rf.lastLogIndex(), term, true
	}
}

// GetState 返回当前任期，以及当前节点是否认为自己是 leader。
// 该方法可能被并发调用，因此读取状态时需要持锁。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// persist 将 Raft 的持久化状态保存到稳定存储，以便节点崩溃重启后恢复。
// 需要持久化的状态见论文图 2。实现快照前，应向 persister.Save()
// 的第二个参数传入 nil；实现快照后，则传入当前快照（尚无快照时仍传 nil）。
// 调用者必须持有 rf.mu，以保证状态编码与快照保存的一致性。
func (rf *Raft) persist() {
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
// 该方法由本地上层状态机调用，不是 RPC；它只裁剪当前节点的日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	if index <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	rf.logEntries = append([]LogEntry{}, rf.logEntries[index-rf.lastIncludedIndex:]...)
	rf.logEntries[0].Command = nil
	// 新切片的第 0 项对应 index，仅保留任期并作为新的哨兵日志。
	rf.snapshot = append([]byte{}, snapshot...)
	rf.lastIncludedIndex = index
	term := rf.logEntries[0].Term
	rf.lastIncludedTerm = term
	rf.persist()
	rf.mu.Unlock()
}

// AppendEntriesArgs 是日志复制和心跳 RPC 的请求参数。
// 其中所有日志索引均为未受快照裁剪影响的绝对索引。
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply 返回当前任期、匹配结果以及快速回退所需的冲突信息。
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries 处理 leader 发来的日志复制或心跳请求。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 拒绝任期已经过期的 leader。
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 发现更高任期后清空本任期的投票记录并持久化。
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	// 合法的 AppendEntries 表示 leader 仍然活跃，应转为 follower 并刷新选举超时。
	rf.role = Follower
	rf.updateElectionDeadline()

	// RPC 使用绝对索引，访问裁剪后的 logEntries 时需转换为 slice 下标。
	localPrevLogIndex := args.PrevLogIndex - rf.lastIncludedIndex
	// PrevLogIndex 超出日志末尾，说明 follower 的日志过短。
	if localPrevLogIndex >= len(rf.logEntries) {
		reply.Success = false
		reply.Term = rf.currentTerm
		// 提示 leader 从 follower 的日志末尾继续尝试。
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.logEntries) + rf.lastIncludedIndex
		return
	}

	// PrevLogIndex 已被本地快照覆盖，提示 leader 从快照边界之后继续。
	if localPrevLogIndex < 0 {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}

	// 前置日志匹配后，追加缺失条目并覆盖第一个冲突条目及其后缀。
	if rf.logEntries[localPrevLogIndex].Term == args.PrevLogTerm {
		for i := range args.Entries {
			localIndex := localPrevLogIndex + 1 + i
			// 本地日志较短，直接追加剩余条目。
			if localIndex >= len(rf.logEntries) {
				rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
				rf.persist()
				break
			}
			// 发现首个任期冲突，截断该位置及其后的日志再追加 leader 后缀。
			if rf.logEntries[localIndex].Term != args.Entries[i].Term {
				rf.logEntries = append(rf.logEntries[:localIndex], args.Entries[i:]...)
				rf.persist()
				break
			}
		}

		// follower 的提交进度不能超过 leader，也不能超过本地日志末尾。
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
			// 唤醒 applier，由其按顺序向状态机交付日志。
			rf.applyCond.Signal()
		}

		reply.Success = true
		reply.Term = rf.currentTerm

	} else {
		// 返回冲突任期及该任期在 follower 日志中的首个索引。
		conflictTerm := rf.logEntries[localPrevLogIndex].Term
		conflictIndex := localPrevLogIndex

		for conflictIndex > 0 && rf.logEntries[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex + rf.lastIncludedIndex
		reply.ConflictTerm = conflictTerm
		reply.Success = false
		reply.Term = rf.currentTerm
	}
}

// RequestVoteArgs 定义 RequestVote RPC 的请求参数。
// 通过 RPC 传输的字段名必须以大写字母开头。
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply 定义 RequestVote RPC 的响应参数。
// 通过 RPC 传输的字段名必须以大写字母开头。
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote 处理其他节点发来的投票请求。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 投票判断和状态更新必须在同一临界区内完成，避免同一任期重复投票。
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		// 更高任期使当前状态失效，节点退回 follower 并清空投票记录。
		rf.becomeFollower(args.Term)
		rf.persist()
		// 仅观察到更高任期并不代表收到合法 leader 的消息，暂不重置选举计时器。
		// rf.updateElectionDeadline()
	}
	// 同任期的 RequestVote 不会使 candidate 或 leader 自动退回 follower。
	lastLogIndex := len(rf.logEntries) - 1
	lastLogTerm := rf.logEntries[lastLogIndex].Term

	// 仅向日志至少与本节点一样新的候选人投票；结合多数派交集，
	// 可保证新 leader 不会缺失已经提交的日志。
	refuseVote := false
	if lastLogTerm > args.LastLogTerm {
		refuseVote = true
	} else if lastLogTerm == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex {
		// 最后任期相同时，再比较最后日志的绝对索引。
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

		// 投票结果属于持久状态，授予投票后立即保存。
		rf.votedFor = args.CandidateID
		rf.persist()
		// 授予投票后刷新计时器，避免立即发起一轮竞争选举。
		rf.updateElectionDeadline()
		return
	}
	// 当前任期已经投给其他候选人，拒绝重复投票。
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

// InstallSnapshotArgs 携带 leader 的快照及其覆盖的日志边界。
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

// InstallSnapshotReply 返回接收方当前任期。
type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot 处理 leader 发来的快照。
// 当 follower 所需日志已被 leader 裁剪时，leader 使用该 RPC 推进其状态。
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 持锁完成任期检查、日志裁剪和持久化，保证快照状态原子更新。
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.updateElectionDeadline()
	}
	// 合法的 InstallSnapshot 也代表 leader 仍然活跃。
	rf.role = Follower
	rf.updateElectionDeadline()

	// 忽略已经安装或被更新快照覆盖的旧请求，避免状态倒退。
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		return
	}

	// 若快照边界在本地日志中存在且任期匹配，则保留其后的有效日志。
	if args.LastIncludedIndex < rf.lastLogIndex() &&
		rf.logEntries[args.LastIncludedIndex-rf.lastIncludedIndex].Term == args.LastIncludedTerm {
		// 复制到新底层数组，使被裁剪日志及其 Command 可由 GC 回收。
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

	// leader 的快照表明至少截至 LastIncludedIndex 的日志已经提交。
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.persist()

	// 将快照交给唯一的 applier；较新的待应用快照可以覆盖旧快照。
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

// startElection 并发请求其他节点投票，并只处理仍属于本轮选举的响应。
func (rf *Raft) startElection() {

	voteReceived := 1

	rf.mu.Lock()
	// 锁内记录本轮选举的任期和最后日志，避免 RPC 期间状态变化影响参数。
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
		// 并发发送 RPC，并通过带缓冲的 results channel 汇总响应。
		go func(serverID int, args *RequestVoteArgs, reply *RequestVoteReply) {
			ok := rf.peers[serverID].Call("Raft.RequestVote", args, reply)
			results <- Result{ok: ok, serverID: serverID, reply: *reply}
		}(serverID, &args, &reply)
	}

	for range len(rf.peers) - 1 {
		result := <-results

		if result.ok {
			rf.mu.Lock()
			// 任意更高任期响应都会使当前节点立即退回 follower。
			if result.reply.Term > rf.currentTerm {
				rf.becomeFollower(result.reply.Term)
				rf.persist()
				rf.mu.Unlock()
				return
			}

			// 角色或任期已改变，说明本轮选举结果已经失效。
			if rf.currentTerm != electionTerm || rf.role != Candidate {
				rf.mu.Unlock()
				return
			}

			if result.reply.VoteGranted {
				voteReceived += 1
			}

			// 获得多数票后立即成为 leader，无需等待其余响应。
			if voteReceived > len(rf.peers)/2 {
				rf.becomeLeader()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}

	// 兼容无需等待其他响应即可形成多数派的场景（例如单节点集群）。
	if voteReceived > len(rf.peers)/2 {
		rf.mu.Lock()
		// RPC 期间状态可能已经变化，成为 leader 前必须再次校验。
		if rf.currentTerm != electionTerm || rf.role != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.becomeLeader()
		rf.mu.Unlock()
	}
	// 未获得多数票时保持 candidate，等待下一次选举超时开始新任期。
}

// sendHeartbeat 为各 follower 触发一次异步复制。
// AppendEntries 无需刻意保持 Entries 为空：follower 落后时可同时补齐日志。
func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	// 锁内确认角色并记录任期，后续 RPC 使用该任期判断请求是否过期。
	term := rf.currentTerm
	rf.mu.Unlock()
	for peer := range len(rf.peers) {
		if peer == rf.me {
			continue
		}
		// 普通复制只发送一次 RPC；若先安装快照，则紧接着尝试一次 AppendEntries。
		go rf.replicateToPeer(peer, term)
	}

	rf.mu.Lock()
	rf.heartbeatDeadline = time.Now().Add(time.Duration(heartbeatInterval) * time.Millisecond)
	rf.mu.Unlock()
}

// ticker 定期检查选举与心跳截止时间，并在锁外启动相应任务。
func (rf *Raft) ticker() {

	for {
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
			// RPC 在独立 goroutine 中执行，避免阻塞 ticker。
			go rf.startElection()
		}
		if needSendHeartbeat {
			rf.sendHeartbeat()
		}
	}
}

// applier 是 applyCh 的唯一发送者，负责按顺序交付快照和已提交日志。
func (rf *Raft) applier() {
	rf.mu.Lock()
	for {
		for rf.lastApplied >= rf.commitIndex && rf.pendingSnapshot == nil {
			// Wait 会原子释放 rf.mu，并在被唤醒后重新获取锁。
			rf.applyCond.Wait()
		}
		// 快照优先于其后的日志交付，避免状态机观察到倒序状态。
		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			// 持锁取出并清空，避免发送期间到达的新快照被误删。
			rf.pendingSnapshot = nil

			rf.mu.Unlock()
			// applyCh 可能阻塞，因此必须在锁外发送。
			rf.applyCh <- msg
			rf.mu.Lock()

			rf.lastApplied = max(rf.lastApplied, msg.SnapshotIndex)
			// 下一轮重新检查是否还有更新快照或待应用日志。
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
		// applyCh 可能阻塞，因此在锁外发送。
		rf.applyCh <- msg
		rf.mu.Lock()
	}
}

// replicateToPeer 向指定 follower 推进一次复制。
// 外层循环仅用于在 InstallSnapshot 成功后立即衔接一次 AppendEntries；
// 普通日志复制无论成功或失败都会返回，以免并发 goroutine 放大 RPC 数量。
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
				// 更高任期响应使当前 leader 立即退回 follower。
				rf.becomeFollower(reply.Term)
				rf.persist()
				rf.mu.Unlock()
				return
			}

			if rf.role != Leader || rf.currentTerm != leaderTerm {
				rf.mu.Unlock()
				return
			}
			// follower 已匹配到快照边界，下一条待发送日志位于边界之后。
			rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
			rf.nextIndex[peer] = max(rf.nextIndex[peer], args.LastIncludedIndex+1)
			// 进入下一轮，立即尝试发送快照之后的日志。
			rf.mu.Unlock()
			continue
		}
		// nextIndex/matchIndex 和 RPC 使用绝对索引；访问 slice 时转换为局部下标。
		nextIndex := rf.nextIndex[peer] - rf.lastIncludedIndex
		prevIndex := nextIndex - 1
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevIndex + rf.lastIncludedIndex,
			PrevLogTerm:  rf.logEntries[prevIndex].Term,
			Entries:      append([]LogEntry{}, rf.logEntries[nextIndex:]...), // 构造 RPC 快照时应复制
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		matched := prevIndex + len(args.Entries) + rf.lastIncludedIndex
		requestNextIndex := rf.nextIndex[peer]
		rf.mu.Unlock()

		// RPC 可能长时间等待或丢包，因此在锁外调用。
		ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			// 更高任期响应使当前 leader 立即退回 follower。
			rf.becomeFollower(reply.Term)
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if rf.role != Leader || rf.currentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// 成功响应只推进复制进度，避免延迟响应使索引倒退。
			rf.matchIndex[peer] = max(rf.matchIndex[peer], matched)
			rf.nextIndex[peer] = max(rf.nextIndex[peer], matched+1)

			// 仅提交已复制到多数节点且属于当前任期的最高日志索引。
			hasNewCommit, newCommitIndex := rf.checkCommitIndex()
			if hasNewCommit {
				// 这里只推进 commitIndex，具体交付由 applier 统一完成。
				rf.commitIndex = newCommitIndex
				rf.applyCond.Signal()
			}
			rf.mu.Unlock()
			return
		} else {
			// 若其他 RPC 已改变 nextIndex，则当前失败响应已经过期。
			if rf.nextIndex[peer] != requestNextIndex {
				rf.mu.Unlock()
				return
			}
			// ConflictTerm 为 -1 表示 follower 没有 PrevLogIndex 对应的日志。
			if reply.ConflictTerm == -1 {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				// 若 leader 含有冲突任期，则跳到该任期最后一条日志之后。
				hasTerm := false
				for i := len(rf.logEntries) - 1; i >= 0; i-- {
					if rf.logEntries[i].Term == reply.ConflictTerm {
						hasTerm = true
						rf.nextIndex[peer] = i + 1 + rf.lastIncludedIndex
						break
					}
				}
				if !hasTerm {
					// leader 不含该任期时，直接跳到 follower 返回的首个冲突索引。
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}
			rf.mu.Unlock()
			return
		}
	}
}

// becomeLeader 将当前节点切换为 leader，并初始化各 follower 的复制进度。
// 调用者必须持有 rf.mu。
func (rf *Raft) becomeLeader() {
	rf.role = Leader
	// leader 使用独立的心跳截止时间驱动周期性复制。
	rf.heartbeatDeadline = time.Now().Add(time.Duration(heartbeatInterval) * time.Millisecond)
	// 新 leader 从自身日志末尾之后开始探测各 follower 的匹配位置。
	for i := range len(rf.nextIndex) {
		rf.nextIndex[i] = len(rf.logEntries) + rf.lastIncludedIndex
		rf.matchIndex[i] = rf.lastIncludedIndex
	}
}

// becomeFollower 切换到指定的新任期，并清空该任期的投票记录。
// 调用者必须持有 rf.mu，且 newTerm 不应小于 currentTerm。
func (rf *Raft) becomeFollower(newTerm int) {
	rf.role = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

// becomeCandidate 开始新一轮选举：递增任期、投票给自己并重置选举超时。
// 调用者必须持有 rf.mu。
func (rf *Raft) becomeCandidate() {
	rf.role = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.updateElectionDeadline()
}

// updateElectionDeadline 设置随机选举截止时间，以降低节点同时参选的概率。
// 调用者必须持有 rf.mu。
func (rf *Raft) updateElectionDeadline() {
	ms := time.Duration(300 + rand.Int63()%200)
	rf.electionDeadline = time.Now().Add(ms * time.Millisecond)
}

// checkCommitIndex 查找可由当前 leader 提交的最高日志索引。
// 只有当前任期且已复制到多数节点的日志才能直接推进 commitIndex。
// 调用者必须持有 rf.mu；函数中的日志索引均为绝对索引。
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
			// N 已复制到多数节点，可以作为新的 commitIndex。
			return true, N
		}
	}
	return false, rf.commitIndex
}

// lastLogIndex 返回当前日志末尾的绝对索引；调用者必须持有 rf.mu。
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.logEntries) - 1
}
