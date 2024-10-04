package raft

import (
	"sync"
	"sync/atomic"
)

func (rf *Raft) FollowerStartElection() {
	rf.mu.Lock()
	DPrintf("[%d] 3 lock success!", rf.me)

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
	rf.mu.Unlock()
	var becomeLeader sync.Once
	var countVotes atomic.Int32
	countVotes.Store(1)
	//wg := sync.WaitGroup{}
	DPrintf("[%d]: term %v start election,Peers %d \n", rf.me, rf.currentTerm, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			rf.mu.Lock()
			rf.resetElectionTimeout()
			rf.mu.Unlock()
			continue
		}

		DPrintf("[%d]: term %v send vote request to %d\n", rf.me, rf.currentTerm, i)
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.log[len(rf.log)-1].Index,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		//wg.Add(1)
		go func(serve int) {
			//defer wg.Done()
			rf.candidateElection(serve, &args, &countVotes, &becomeLeader)

		}(i)
	}
	//wg.Wait()
}

func (rf *Raft) resetLeaderState() {
	DPrintf("[%d] reset leader state，rf.nextIndex = %d", rf.me, len(rf.log))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}
