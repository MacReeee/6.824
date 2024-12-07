package raft

import (
	"log"
	"math/rand"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期
	CandidateId  int // 请求选票的候选人ID
	LastLogIndex int // 候选人的最后一条日志的索引
	LastLogTerm  int // 候选人的最后一条日志的任期
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果请求的任期小于当前任期，则拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Println("节点", rf.me, ": 拒绝投票, 任期过旧. 请求任期:", args.Term, "当前任期:", rf.currentTerm)
		return
	}

	// 如果请求的任期大于当前任期，则更新当前任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.voteFor = -1
		rf.persist()
	}

	// 如果候选人的日志不够新，则拒绝投票
	if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term || (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)-1) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Println("节点", rf.me, ": 拒绝投票, 日志不够新. 请求日志:", args.LastLogIndex, args.LastLogTerm, "当前日志:", len(rf.logs)-1, rf.logs[len(rf.logs)-1].Term)
		return
	}

	// 如果当前节点没有投票，则投票给候选人
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.voteFor = args.CandidateId
		rf.resetElectionTicker()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.persist()
		return
	}

	// 否则拒绝投票
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	log.Println("节点", rf.me, ": 拒绝投票, 已投票给", rf.voteFor)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期
	VoteGranted bool // 是否投票
}

func (rf *Raft) requestVoteReplyHandler(server int, reply *RequestVoteReply) {
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

	// 如果当前节点不是候选人，则忽略
	if rf.role != Candidate {
		return
	}

	// 如果当前节点的任期大于回复的任期，则忽略
	if reply.Term < rf.currentTerm {
		return
	}

	// 如果收到了投票，则增加投票数
	if reply.VoteGranted && !rf.voters[server] {
		rf.voters[server] = true
		rf.votedCount++
		if rf.votedCount > len(rf.peers)/2 && rf.role == Candidate {
			rf.role = Leader
			rf.leaderId = rf.me
			rf.nextsIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.nextsIndex {
				rf.nextsIndex[i] = len(rf.logs)
				rf.matchIndex[i] = 0
			}
			rf.persist()
		}
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) resetElectionTicker() {
	ms := 50 + (rand.Int63() % 300)
	rf.electionTicker.Reset(electionTimeout + time.Duration(ms)*time.Millisecond)
}
