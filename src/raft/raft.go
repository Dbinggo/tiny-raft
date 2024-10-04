package raft

//
// 这是 Raft 必须向服务（或测试者）公开的 API 概述。有关每个函数的更多详细信息，请参见下面的注释。
//
// rf = Make(...)
//   创建一个新的 Raft 服务器。
// rf.Start(command interface{}) (index, term, isleader)
//   开始对一个新的日志条目达成一致
// rf.GetState() (term, isLeader)
//   询问 Raft 当前的任期，以及它是否认为自己是领导者
// ApplyMsg
//   每次有新的条目提交到日志时，每个 Raft 节点都应该向同一服务器中的服务（或测试者）发送一个 ApplyMsg。
//

import (
	"6.5840/labgob"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// / 一个实现单个 Raft 节点的 Go 对象。
type Raft struct {
	mu        sync.Mutex          // 锁，用于保护该节点状态的共享访问
	peers     []*labrpc.ClientEnd // 所有节点的 RPC 端点
	persister *Persister          // 保存该节点持久化状态的对象
	me        int                 // 该节点在 peers[] 中的索引
	dead      int32               // 通过 Kill() 设置

	// 你的数据在这里 (3A, 3B, 3C)。
	// 查看论文的图 2 以了解 Raft 服务器必须维护的状态。
	// Persistent state on all servers:
	//所有服务器上的持久状态：
	currentTerm int
	votedFor    int
	log         []Entry
	leaderId    int

	// Volatile state on all servers:
	//所有服务器上的易失性状态：
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	state        State      // 0 follower, 1 candidate, 2 leader
	applyCond    *sync.Cond // to notify the server to apply the log
	applyCh      chan ApplyMsg
	electionTime time.Time // to start election
}

// 返回当前任期以及该服务器是否认为自己是领导者。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	DPrintf("[%d] 7 lock success!", rf.me)
	defer rf.mu.Unlock()
	DPrintf("[%d] [%d] state %d, commitedIndex %d , applyIndex %d , log %v ", rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.log)

	var term int
	var isleader bool
	// 你的代码在这里 (3A)。
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// 将 Raft 的持久化状态保存到稳定存储中，
// 以便在崩溃和重启后可以恢复。
// 查看论文的图 2 以了解应该持久化的内容。
// 在你实现快照之前，你应该将 nil 作为第二个参数传递给 persister.Save()。
// 在你实现快照之后，传递当前的快照（如果还没有快照，则传递 nil）。
func (rf *Raft) persist() {
	// 你的代码在这里 (3C)。
	// 示例：

	DPrintf("[%d] 8 lock success!", rf.me)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// 恢复之前持久化的状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // 没有任何状态的引导？
		return
	}
	// 你的代码在这里 (3C)。
	// 示例：
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []Entry
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf("[%d] readPersist error", rf.me)
	} else {
		rf.mu.Lock()
		DPrintf("[%d] 9 lock success!", rf.me)
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
		rf.mu.Unlock()
		DPrintf("[%d] readPersist success", rf.me)
	}
	return

}

// 服务表示它已经创建了一个包含所有信息的快照，
// 包括索引。这意味着服务不再需要通过（并包括）该索引的日志。
// Raft 现在应该尽可能地修剪其日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// 使用 Raft 的服务（例如 k/v 服务器）希望开始对要附加到 Raft 日志的下一个命令达成一致。
// 如果该服务器不是领导者，则返回 false。否则，开始达成一致���立即返回。
// 不能保证该命令会被提交到 Raft 日志中，因为领导者可能会失败或失去选举。
// 即使 Raft 实例已被终止，此函数也应正常返回。
//
// 第一个返回值是该命令如果被提交将出现在的索引。
// 第二个返回值是当前任期。
// 第三个返回值是该服务器是否认为自己是领导者。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	if rf.killed() {
		return index, term, isLeader
	}
	if rf.state == Leader {
		// Your code here (3B).
		rf.mu.Lock()
		defer rf.mu.Unlock()
		index = len(rf.log)
		term = rf.currentTerm
		isLeader = true
		rf.appendEntries(Entry{Term: term, Index: index, Command: command})
		DPrintf("[%d] [%d]leader start to append entries %v", rf.me, rf.currentTerm, Entry{Term: term, Index: index, Command: command})
		rf.persist()
		rf.sendAllAppendEntries(false)
	} else {
		DPrintf("[%d] not leader", rf.me)
		return index, term, isLeader
	}
	// Your code here (3B).
	return index, term, isLeader
}

// 测试者不会在每次测试后停止 Raft 创建的 goroutine，
// 但它会调用 Kill() 方法。你的代码可以使用 killed() 来
// 检查是否已调用 Kill()。使用 atomic 避免了需要锁。

// 问题在于长时间运行的 goroutine 会占用内存并可能消耗 CPU 时间，
// 这可能导致后续测试失败并生成混乱的调试输出。任何具有长时间运行循环的 goroutine
// 都应该调用 killed() 来检查是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("[%d] kill", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		DPrintf("[%d] [%d] state %d, commitedIndex %d , applyIndex %d , log %v ", rf.me, rf.currentTerm, rf.state, rf.commitIndex, rf.lastApplied, rf.log)
		switch rf.state {
		case Leader:
			rf.sendAllAppendEntries(true)
		case Candidate:
			if time.Now().After(rf.electionTime) {
				// start to election
				DPrintf("[%d] start election", rf.me)
				rf.FollowerStartElection()
			}
		case Follower:
			if time.Now().After(rf.electionTime) {
				// start to election
				DPrintf("[%d] start election", rf.me)
				rf.FollowerStartElection()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// 服务或测试者希望创建一个 Raft 服务器。所有 Raft 服务器（包括这个）的端口都在 peers[] 中。
// 这个服务器的端口是 peers[me]。所有服务器的 peers[] 数组顺序相同。
// persister 是一个保存该服务器持久化状态的地方，并且最初持有最近保存的状态（如果有的话）。
// applyCh 是一个通道，测试者或服务期望 Raft 通过该通道发送 ApplyMsg 消息。
// Make() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderId = -1
	rf.log = make([]Entry, 0)
	rf.appendEntries(Entry{Term: 0, Index: 0}) // to make sure the first index is 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.mu = sync.Mutex{}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rf.resetElectionTimeout()
	DPrintf("[%d] start", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsg()
	return rf
}
