package raft

import (
	"math/rand"
	"time"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	go rf.startElectionTicker()
	go rf.startHeartbeatTicker()
}

func (rf *Raft) startElectionTicker() {
	rf.mu.Lock()
	ms := 50 + (rand.Int63() % 300)
	rf.electionTicker = time.NewTicker(time.Millisecond * time.Duration(ms))
	electionTicker := rf.electionTicker
	rf.mu.Unlock()

	for range electionTicker.C {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}

		// 开始选举
		rf.currentTerm++
		rf.role = Candidate
		rf.voteFor = rf.me
		rf.votedCount = 1
		rf.voters = make([]bool, len(rf.peers))
		rf.persist()
		rf.resetElectionTicker()

		//log.Println("节点", rf.me, "开始选举,任期:", rf.currentTerm)

		// 构造投票请求
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.logs) - 1,
			LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		}

		// 发送投票请求
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				reply := &RequestVoteReply{}
				if !rf.sendRequestVote(server, args, reply) {
					//log.Println("sendRequestVote RPC 错误")
					return
				}
				//log.Println("节点", rf.me, "收到来自节点", server, "的投票,结果:", reply)
				rf.requestVoteReplyHandler(server, reply)
			}(server)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startHeartbeatTicker() {
	heartBeatTicker := time.NewTicker(heartbeatTimeout)
	defer heartBeatTicker.Stop()
	for range heartBeatTicker.C {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.role != Leader {
			rf.mu.Unlock()
			continue
		}
		// 发送心跳
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go rf.sendAndHandleAppendEntries(server, true)
		}
		rf.mu.Unlock()
	}
}
