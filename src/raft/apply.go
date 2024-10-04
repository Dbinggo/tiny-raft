package raft

// 当每个 Raft 节点意识到连续的日志条目被提交时，
// 该节点应通过传递给 Make() 的 applyCh 向同一服务器上的服务（或测试者）发送一个 ApplyMsg。
// 将 CommandValid 设置为 true 以表明 ApplyMsg 包含一个新提交的日志条目。
//
// 在第 3D 部分中，你将希望在 applyCh 上发送其他类型的消息（例如，快照），
// 但对于这些其他用途，将 CommandValid 设置为 false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// applyMsg 用于将已提交的日志应用到状态机
func (rf *Raft) applyMsg() {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			return
		}
		// locking
		DPrintf("[%d] apply msg from %d to %d", rf.me, rf.lastApplied, rf.commitIndex)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			// apply msg
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			DPrintf("[%d] apply msg %v", rf.me, msg)
			rf.applyCh <- msg
			rf.lastApplied = i
			rf.persist()
		}
		rf.mu.Unlock()
	}
}
