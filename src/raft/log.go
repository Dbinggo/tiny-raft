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
	DPrintf("[%d] term %d receive append entries from %d, now logs %v，args %v", rf.me, args.Term, args.LeaderId, rf.log, args)
	defer rf.mu.Unlock()
	defer func() {
		DPrintf("[%d] term %d append entries from %d, success %v , now logs %v", rf.me, args.Term, args.LeaderId, reply.Success, rf.log)
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
		DPrintf("[%d] [%d] log doesn't contain an entry at prevLogIndex %d , log %v , args log %v", rf.me, rf.currentTerm, args.PrevLogIndex, rf.log, args.Entries)
		reply.Term = rf.currentTerm
		return
	} else if len(args.Entries) != 0 {
		//If an existing entry conflicts with a new one (same index
		//but different terms), delete the existing entry and all that
		//follow it (§5.3)
		reply.Success = true
		rf.log = rf.log[:args.PrevLogIndex+1] // delete the existing entry
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
	DPrintf("[%d] [%d] send append entries to %d , args %v", rf.me, rf.currentTerm, sever, args)
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

		go func(server int) {
		AGAIN:
			rf.mu.Lock()
			var args = AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
			if !heartBeat { // heartbeat 不需要发送日志
				args = AppendEntriesArgs{
					Term:         rf.currentTerm,
					PrevLogIndex: rf.nextIndex[server] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
					Entries:      make([]Entry, len(rf.log[rf.nextIndex[server]:])),
					LeaderCommit: rf.commitIndex,
				}
				copy(args.Entries, rf.log[rf.nextIndex[server]:])
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			if rf.killed() {
				DPrintf("[%d] killed", rf.me)
				return
			}
			if rf.state != Leader {
				return
			}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				return
			}
			if reply.Success {
				rf.mu.Lock()
				DPrintf("[%d] 6 lock success!", rf.me)
				if !heartBeat {
					// 更新 nextIndex 和 matchIndex
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1 // 下一个要发送的日志
					rf.matchIndex[server] = rf.nextIndex[server] - 1                 // 已经复制的日志
					DPrintf("[%d] [%d] %d append entries success, nextIndex %v, matchIndex %v\n", rf.me, rf.currentTerm, server, rf.nextIndex, rf.matchIndex)
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
				}
				rf.mu.Unlock()
			} else if reply.Term > rf.currentTerm {
				rf.mu.Lock()
				// 如果 AppendEntries 失败，减少 nextIndex 并重试
				DPrintf("[%d] [%d] %d 在新的term，更新term，结束\n", rf.me, rf.currentTerm, server)
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				rf.nextIndex[server]-- // feat: 在论文中可以用二分法加快速度，但是raft作者觉得这是没有必要的，本来发生概率就会非常小
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
				DPrintf("[%d] [%d] %d append entries failed, retry\n", rf.me, rf.currentTerm, server)
				heartBeat = false
				rf.mu.Unlock()
				goto AGAIN // 重试
			}
		}(i)
	}
}
