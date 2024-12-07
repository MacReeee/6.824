package raft

import "sort"

type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader的ID
	PrevLogIndex int        // 新日志的前一条日志的索引
	PrevLogTerm  int        // 新日志的前一条日志的任期
	Entries      []LogEntry // 要追加的日志条目
	LeaderCommit int        // leader的提交索引
}

type AppendEntriesReply struct {
	Term          int  // 当前任期，用于更新leader的任期
	Success       bool // 是否成功
	ConflictIndex int  // 冲突的日志索引
	ConflictTerm  int  // 冲突的日志任期
}

func (rf *Raft) sendAndHandleAppendEntries(server int, isHeartBeat bool) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	// 构造请求
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextsIndex[server] - 1,
		PrevLogTerm:  rf.logs[rf.nextsIndex[server]-1].Term,
		Entries:      nil,
		LeaderCommit: rf.committedIndex,
	}

	// 如果不是心跳，则追加日志
	if !isHeartBeat {
		args.Entries = make([]LogEntry, len(rf.logs)-rf.nextsIndex[server])
		copy(args.Entries, rf.logs[rf.nextsIndex[server]:])
	}
	rf.mu.Unlock()

	// 发送请求
	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(server, args, reply) {
		return
	}

	// 处理回复
	rf.appendEntriesReplyHandler(server, args, reply)

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果请求的任期小于当前任期，则拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 如果请求的任期大于当前任期，则更新当前任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.voteFor = -1
		rf.persist()
	}

	// 重置选举定时器
	rf.resetElectionTicker()

	// 如果leader的前一条日志不存在，则拒绝
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		// 提供冲突信息
		if args.PrevLogIndex >= len(rf.logs) { // 落后
			reply.ConflictIndex = len(rf.logs)
			reply.ConflictTerm = -1
		} else { // 冲突
			reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
			// 找到冲突的日志索引
			for i := args.PrevLogIndex; i > 0 && rf.logs[i].Term == reply.ConflictTerm; i-- {
				reply.ConflictIndex = i
			}
		}
		return
	}

	// 删除冲突的日志并追加新日志
	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)

	// 更新提交索引
	if args.LeaderCommit > rf.committedIndex {
		rf.committedIndex = min(args.LeaderCommit, len(rf.logs)-1)
		rf.notifyApply()
	}

	// 返回成功
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) appendEntriesReplyHandler(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果当前节点的任期小于回复的任期，则更新当前节点的任期
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.voteFor = -1
		rf.persist()
		return
	}

	// 如果Term不匹配，说明为过时的RPC回复
	if reply.Term < rf.currentTerm {
		return
	}

	// 如果成功，则更新nextIndex和matchIndex
	if reply.Success {
		rf.nextsIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		// 更新提交索引
		rf.updateCommitIndex()
	} else {
		// 处理冲突
		rf.nextsIndex[server] = reply.ConflictIndex
	}
}

func (rf *Raft) updateCommitIndex() {
	// 收集 matchIndex 并排序
	sortedMatchIndex := make([]int, len(rf.peers))
	copy(sortedMatchIndex, rf.matchIndex)
	sort.Ints(sortedMatchIndex)

	// 找到大多数节点复制的最大索引
	majorityIndex := sortedMatchIndex[len(rf.peers)/2]

	// 检查任期是否匹配，满足 Raft 的安全性
	if majorityIndex > rf.committedIndex && rf.logs[majorityIndex].Term == rf.currentTerm {
		rf.committedIndex = majorityIndex
		rf.notifyApply()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
