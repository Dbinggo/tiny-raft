package raft

import (
	"sync/atomic"
)

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) appendEntries(entry ...Entry) {

	rf.log = append(rf.log, entry...)
}

func (rf *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// lock
	rf.mu.Lock()
	//DPrintf("[%d] 5 lock success!", rf.me)
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("[%d] term %d append entries from %d, success %v", rf.me, args.Term, args.LeaderId, reply.Success)
	}()
	if rf.killed() {
		return
	}
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// reFresh election timer
	rf.resetElectionTimeout()
	DPrintf("[%d] [%d] receive append entries from %d", rf.me, args.Term, args.LeaderId)
	DPrintf("[%d] [%d] reset times", rf.me, rf.currentTerm)
	rf.state = Follower
	rf.leaderId = args.LeaderId
	// Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it (§5.3)
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.log = rf.log[:args.PrevLogIndex] // delete the existing entry
		DPrintf("[%d] [%d] delete the existing entry", rf.me, rf.currentTerm)
	}
	//  Append any new entries not already in the log
	if len(args.Entries) > 0 {
		rf.appendEntries(args.Entries...)
		DPrintf("[%d] [%d] append new entries %v", rf.me, rf.currentTerm, args.Entries)
	}

	//  If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		rf.applyCond.Broadcast() // wake up apply thread
	}

	rf.persist()
	reply.Success = true
	reply.Term = args.Term
	return
}
func (rf *Raft) sendAppendEntries(sever int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[sever].Call("Raft.AppendEntriesRPC", args, reply)
	return ok
}
func (rf *Raft) sendAllAppendEntries(heartBeat bool) {
	var counter atomic.Int32
	counter.Store(1) // leader 自己已经复制了日志
	for i := 0; i < len(rf.peers); i++ {
		DPrintf("[%d] [%v] send append entries to %d\n", rf.me, rf.currentTerm, i)
		if i == rf.me {
			rf.resetElectionTimeout()
			continue
		}
		var args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}
		if !heartBeat { // heartbeat 不需要发送日志
			args = AppendEntriesArgs{
				Term:         rf.currentTerm,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				Entries:      rf.log[rf.nextIndex[i]:],
				LeaderCommit: rf.commitIndex,
			}
		}

		reply := AppendEntriesReply{}
		go func(server int) {
		AGAIN:
			if rf.killed() {
				DPrintf("[%d] killed", rf.me)
				return
			}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok && !heartBeat {
				rf.mu.Lock()
				DPrintf("[%d] 6 lock success!", rf.me)
				defer rf.mu.Unlock()
				if !heartBeat && reply.Success {
					// 更新 nextIndex 和 matchIndex
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1 // 下一个要发送的日志
					rf.matchIndex[server] = rf.nextIndex[server] - 1                 // 已经复制的日志
					counter.Add(1)
					if counter.Load() > int32(len(rf.peers)/2) { // 大多数的服务器都复制了日志
						DPrintf("[%d] [%d] update commitIndex\n", rf.me, rf.currentTerm)
						// 如果存在一个满足 N > commitIndex 的 N，并且大多数的 matchIndex[i] ≥ N 成立，
						// 并且 log[N].term == currentTerm 成立，那么令 commitIndex 等于这个 N
						for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
							count := 1
							for i := 0; i < len(rf.peers); i++ {
								if rf.matchIndex[i] >= N {
									count++
								}
							}
							if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
								rf.commitIndex = N
								rf.applyCond.Broadcast() // wake up apply thread
								break
							}
						}

					}
				} else if reply.Term > rf.currentTerm {
					// 如果 AppendEntries 失败，减少 nextIndex 并重试

					DPrintf("[%d] [%d] %d 在新的term，更新term，结束\n", rf.me, rf.currentTerm, server)
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
				} else {
					rf.nextIndex[server]-- // feat: 在论文中可以用二分法加快速度，但是raft作者觉得这是没有必要的，本来发生概率就会非常小
					DPrintf("[%d] [%d] %d append entries failed, retry\n", rf.me, rf.currentTerm, server)
					goto AGAIN // 重试
				}

			}
		}(i)
	}
}
