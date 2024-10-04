package raft

import (
	"sync"
	"sync/atomic"
)

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// 示例 RequestVote RPC 回复结构。
// 字段名必须以大写字母开头！
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
	From        int
}

/*
	投票由一个称为 RequestVote 的 RPC 调用进行，请求中除了有 Candidate自己的 term 和 id 之外，
	还要带有自己最后一个日志条目的 index 和 term。Candidate首先会给自己投票，然后再向其他节点收集投票信息，
	收到投票信息的节点，会利用如下规则判断是否投票：
	1. 首先会判断请求的term是否更大，不是则说明是旧消息，拒绝该请求。
	2. 如果任期Term相同，则比较index，index较大则为更加新的日志；如果任期Term不同，term更大的则为更新的消息。如果是更新的消息，则给Candidate投票。
*/
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// lock
	rf.mu.Lock()
	DPrintf("[%d] 1 lock success!", rf.me)
	defer rf.mu.Unlock()
	defer func() {
		rf.applyCond.Broadcast()
		DPrintf("[%d] [%d] vote for %d, agree %v", rf.me, args.Term, args.CandidateId, reply.VoteGranted)
	}()
	if rf.killed() {
		reply.VoteGranted = false
		reply.From = rf.me
		return
	}

	// Update term if necessary
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term // update term
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// vote for other
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.From = rf.me
		return
	}

	// if votedFor is null or votedFor is candidate and candidate's log is up-to-date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index)) {
		rf.votedFor = args.CandidateId
		rf.resetElectionTimeout()
		reply.VoteGranted = true
		rf.persist()
		reply.From = rf.me
		return
	} else {
		reply.VoteGranted = false
	}

	// if votedFor is not null and votedFor is not candidate or candidate's log is not up-to-date
	reply.VoteGranted = false
	reply.From = rf.me
	return
}

// 示例代码，用于向服务器发送 RequestVote RPC。
// server 是 rf.peers[] 中目标服务器的索引。
// args 中包含 RPC 参数。
// *reply 中包含 RPC 回复，因此调用者应该传递 &reply。
// 传递给 Call() 的 args 和 reply 的类型必须与处理函数中声明的参数类型相同（包括它们是否是指针）。
//
// labrpc 包模拟了一个丢包网络，其中服务器可能无法访问，请求和回复可能会丢失。
// Call() 发送请求并等待回复。如果在超时间隔内收到回复，Call() 返回 true；否则 Call() 返回 false。因此 Call() 可能不会立即返回。
// false 返回值可能是由于服务器宕机、无法访问的活跃服务器、丢失的请求或丢失的回复引起的。
//
// Call() 保证返回（可能会有延迟）*除非*服务器端的处理函数没有返回。因此不需要在 Call() 周围实现自己的超时机制。
//
// 查看 ../labrpc/labrpc.go 中的注释以获取更多详细信息。
//
// 如果你在使 RPC 工作时遇到问题，请检查你是否已将通过 RPC 传递的结构体中的所有字段名大写，并且调用者传递的是回复结构体的地址（&），而不是结构体本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) candidateElection(server int, args *RequestVoteArgs, countVotes *atomic.Int32, becomeLeader *sync.Once) {
	DPrintf("[%d] [%d]  send vote request to %d\n", rf.me, args.Term, server)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	DPrintf("[%d] 2 lock success!", rf.me)
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		DPrintf("[%d] [%d] %d 在新的term，更新term，结束\n", rf.me, args.Term, server)
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}
	if reply.Term < args.Term {
		DPrintf("[%d] [%d] %d 的term %d 已经失效，结束\n", rf.me, args.Term, server, reply.Term)
		return
	}
	if !reply.VoteGranted {
		DPrintf("[%d] [%d] %d 没有投给me，结束\n", rf.me, args.Term, server)
		return
	}
	DPrintf("[%d] [%d] from %d term一致，且投给%d\n", rf.me, args.Term, server, rf.me)
	countVotes.Add(1)
	DPrintf("[%d] [%d] countVotes %d , state %d currentTerm %d\n", rf.me, args.Term, countVotes.Load(), rf.state, rf.currentTerm)
	if countVotes.Load() > int32(len(rf.peers)/2) && rf.state == Candidate && rf.currentTerm == args.Term {
		DPrintf("[%d] [%d] 成为leader\n", rf.me, args.Term)
		becomeLeader.Do(func() {
			rf.leaderId = rf.me
			rf.state = Leader
			rf.resetElectionTimeout()

			rf.resetLeaderState()
			rf.sendAllAppendEntries(false)
		})
	}

}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTime = randomElectionTimeout()
}
