package raft

//
// this is an outline of the APIthat raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

// ApplyMsg is
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the ApplyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the ApplyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type ServerState string

const (
	Follower  ServerState = "Follower"
	Candidate ServerState = "Candidate"
	Leader    ServerState = "Leader"
)

// Raft is
// A Go object implementing a single Raft peer.
//
type Log struct {
	Term    int
	Command interface{}
}
type Raft struct {
	Mu        sync.Mutex          // Lock to protect shared access to this peer's State
	Peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted State
	Me        int                 // this peer's index into peers[]

	heartbeatTimer *time.Timer
	CurrentTerm    int
	VotedFor       int
	log            map[int]Log
	State          ServerState
	replyCh        chan bool

	leaderTriggerTimer *time.Timer

	ApplyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func stateTransition1(votedFor int, currentTerm int, rf *Raft) {
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	DPrintf("In stateTransition1: Current Term is changed to %d and voted for to %d", rf.CurrentTerm, rf.VotedFor)

}

func stateTransition(state ServerState, VotedFor int, rf *Raft) {
	rf.State = state
	rf.VotedFor = VotedFor
	DPrintf("In stateTransition: State is changed to %v and voted for to %d", rf.State, rf.VotedFor)
}

func (rf *Raft) GetState() (int, bool) {
	rf.Mu.Lock()
	Term := rf.CurrentTerm
	Isleader := (rf.State == Leader)
	rf.Mu.Unlock()
	DPrintf("In GetState: Term is changed to %d and isLeader to %v", Term, Isleader)
	return Term, Isleader
}

func (rf *Raft) persist() {
}

//
// save Raft's persistent State to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

//
// restore previously persisted State.
//

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Entries  []Log
}

type AppendEntriesReply struct {
	Term          int
	ConflictIndex int
	Success       bool
}

func (rf *Raft) RequestAppend(appendArg *AppendEntriesArgs, appendReply *AppendEntriesReply) {
	rf.Mu.Lock()
	if appendArg.Term < rf.CurrentTerm {
		appendReply.Term = rf.CurrentTerm
		appendReply.Success = false
		DPrintf("In RequestAppend: appendReply.Term is changed to %d and appendReply.Success to %v", appendReply.Term, appendReply.Success)
		rf.Mu.Unlock()
		return
	}
	if appendArg.Term > rf.CurrentTerm {
		stateTransition1(-1, appendArg.Term, rf)
		if rf.State == Leader {
			go transitionFromLeader(rf)
		} else if rf.State == Candidate {
			go transitionToFollower(rf)
		} else {
			go transitionToFollower(rf)
		}
		DPrintf("In RequestAppend: appendReply.Term > rf.CurrentTerm block")
	}
	rf.leaderTriggerTimer.Stop()
	rf.leaderTriggerTimer.Reset(timeDuration())

	rf.CurrentTerm = appendArg.Term
	appendReply.Term = rf.CurrentTerm
	appendReply.Success = true
	DPrintf("In RequestAppend: appendReply.Term becomes %d, appendReply.Success becomes %v", appendReply.Term, appendReply.Success)
	rf.Mu.Unlock()
	return
}

func accostReply(rf *Raft, inputidx int,
	appendArg AppendEntriesArgs, appendReply []AppendEntriesReply) {
	if rf.Peers[inputidx].Call("Raft.RequestAppend", &appendArg, &appendReply[inputidx]) {
		rf.replyCh <- true
	}
	rf.replyCh <- false
	DPrintf("In accostReply: data sent via replyCh channel")

}

func callRequestAppend(rf *Raft) {
	rf.Mu.Lock()
	if rf.State != Leader {
		DPrintf("In callRequestAppend: rf.State is %v", rf.State)
		return
	}
	replyArr := make([]AppendEntriesReply, len(rf.Peers))
	rf.Mu.Unlock()
	for index := 0; index < len(rf.Peers); index++ {
		if index != rf.Me {
			go func(inputidx int) {
				if rf.State != Leader {
					DPrintf("In callRequestAppend For loop: rf.State is %v", rf.State)
					return
				}
				appendArgs := AppendEntriesArgs{
					LeaderId: rf.Me,
					Entries:  []Log{},
					Term:     rf.CurrentTerm,
				}
				accostReply(rf, inputidx, appendArgs, replyArr)
				if !<-rf.replyCh {
					DPrintf("In callRequestAppend: received channel input")
					return
				} else {
					if replyArr[inputidx].Term > rf.CurrentTerm {
						rf.Mu.Lock()
						DPrintf("In callRequestAppend: replyArr[inputidx].Term is %d > rf.CurrentTerm %d", replyArr[inputidx].Term, rf.CurrentTerm)
						transitionFromLeader(rf)
						rf.Mu.Unlock()
						return
					}
				}
			}(index)
		}
	}
}

// RequestVoteArgs is
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// RequestVoteReply is
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool

	// Your data here (2A).
}

// AppendEntries is
type AppendEntries struct {
	Term     int
	LeaderID int
}

// RequestVote is
// example RequestVote RPC handler.
//
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Mu.Lock()
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm {
		DPrintf("In RequestVote: args.Term is %d < rf.CurrentTerm is %d", args.Term, rf.CurrentTerm)
		rf.Mu.Unlock()
		return
	}
	if args.Term > rf.CurrentTerm {
		stateTransition1(-1, args.Term, rf)
		if rf.State == Candidate {
			go transitionToFollower(rf)
		}
		if rf.State == Leader {
			go transitionFromLeader(rf)
		}
		DPrintf("In RequestVote: args.Term is %d > rf.CurrentTerm is %d", args.Term, rf.CurrentTerm)

	}
	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
		DPrintf("In RequestVote: rf.VotedFor %v", rf.VotedFor)
		rf.Mu.Unlock()
		return
	}
	rf.leaderTriggerTimer.Stop()
	rf.leaderTriggerTimer.Reset(timeDuration())
	reply.VoteGranted = true
	stateTransition1(args.CandidateId, args.Term, rf)
	rf.Mu.Unlock()
	return
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Mu.Lock()
	index := -1
	term := rf.CurrentTerm
	checkLeader := rf.State == Leader
	DPrintf("In Start: index is %d, term is %d, checkleader is %v", index, term, checkLeader)
	rf.Mu.Unlock()
	return index, term, checkLeader
}

// Kill is
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
}

// Make is
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent State, and also initially holds the most
// recent saved State, if any. ApplyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func transitionToFollower(rf *Raft) {
	rf.Mu.Lock()
	if rf.State == Follower {
		DPrintf("In transitionToFollower: rf.State is %v", rf.State)
		rf.Mu.Unlock()
		return
	}
	stateTransition(Follower, -1, rf)
	rf.leaderTriggerTimer.Stop()
	rf.leaderTriggerTimer.Reset(timeDuration())
	rf.Mu.Unlock()
}

func waitForTimer(rf *Raft) {
	for {
		<-rf.heartbeatTimer.C
		rf.heartbeatTimer.Stop()
		rf.heartbeatTimer.Reset(heartbeatOut(HeartBeatInterval))
		DPrintf("In waitForTimer: signal unblocked")
		callRequestAppend(rf)

	}
}

func heartbeatOut(interval int) time.Duration {
	return time.Duration(interval) * time.Millisecond
}

func transitionToLeader(rf *Raft) {
	if rf.State == Leader {
		DPrintf("In transitionToLeader: rf.State is %v", rf.State)
		return
	}
	rf.Mu.Lock()
	rf.State = Leader
	rf.Mu.Unlock()
	rf.heartbeatTimer = time.NewTimer(time.Duration(0))
	go waitForTimer(rf)
}

func transitionFromLeader(rf *Raft) {
	rf.Mu.Lock()
	if rf.State == Follower {
		DPrintf("In transitionFromLeader: rf.State is %v", rf.State)
		rf.Mu.Unlock()
		return
	}
	stateTransition(Follower, -1, rf)
	rf.heartbeatTimer.Stop()
	rf.leaderTriggerTimer.Stop()
	rf.leaderTriggerTimer.Reset(timeDuration())
	rf.Mu.Unlock()
}

func statecheck(rf *Raft) {
	rf.Mu.Lock()
	if rf.State != Candidate {
		rf.Mu.Unlock()
		DPrintf("In statecheck: rf.State is %v", rf.State)
		return
	}
	rf.Mu.Unlock()
}

func electSubroutine(index int, rf *Raft, voteCounter *int,
	args RequestVoteArgs, voteReplyArr []RequestVoteReply) {
	if rf.State != Candidate {
		DPrintf("In electSubroutine: rf.State is %v", rf.State)
		return
	}
	ok := rf.Peers[index].Call("Raft.RequestVote", &args, &voteReplyArr[index])
	if ok == false {
		return
	}
	go statecheck(rf)
	if args.Term != rf.CurrentTerm {
		DPrintf("In electSubroutine: args.Term is %v", args.Term)
		return
	}
	if voteReplyArr[index].Term > rf.CurrentTerm {
		DPrintf("In electSubroutine: voteReplyArr[index].Term is %d", voteReplyArr[index].Term)
		go transitionToFollower(rf)
		return
	}
	if voteReplyArr[index].VoteGranted == true {
		*voteCounter++
		if *voteCounter >= (len(rf.Peers)/2 + 1) {
			DPrintf("In electSubroutine: voteReplyArr[index].VoteGranted is %v", voteReplyArr[index].VoteGranted)
			transitionToLeader(rf)
			return
		}

	}
}

func beginElection(rf *Raft) {
	rf.Mu.Lock()
	voteCounter := rf.Me
	args := RequestVoteArgs{Term: rf.CurrentTerm, CandidateId: rf.Me}
	voteReplyArr := make([]RequestVoteReply, len(rf.Peers))
	rf.Mu.Unlock()
	rf.Mu.Lock()
	for index := 0; index < len(rf.Peers); index++ {
		if index != rf.Me {
			DPrintf("In beginElection: index is %d", index)
			go electSubroutine(index, rf, &voteCounter, args, voteReplyArr)
		}
	}
	rf.Mu.Unlock()

}

const (
	HeartBeatInterval = 95
)

func timeDuration() time.Duration {
	return time.Duration(rand.Intn(250)+250) * time.Millisecond
}

func initElection(rf *Raft) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if rf.State == Leader {
		DPrintf("In initElection: rf.State is %v", rf.State)
		return
	}
	stateTransition(Candidate, rf.Me, rf)
	rf.CurrentTerm++
	DPrintf("In initElection: rf.CurrentTerm is %d", rf.CurrentTerm)
	rf.leaderTriggerTimer.Reset(timeDuration())
	go beginElection(rf)
}

func checktimeout(rf *Raft) {
	for {
		<-rf.leaderTriggerTimer.C
		DPrintf("In checktimeout: signal unblocked")
		go initElection(rf)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, ApplyCh chan ApplyMsg) *Raft {
	rf := &Raft{Peers: peers, Persister: persister, Me: me, State: Follower, log: make(map[int]Log), VotedFor: -1, CurrentTerm: 0, leaderTriggerTimer: time.NewTimer(timeDuration())}
	rf.replyCh = make(chan bool)
	go checktimeout(rf)
	return rf
}

// type RequestVoteArgs struct {
// 	 Your data here (2A, 2B).
// 	Term int
// 	CandidateId int

// }
// type RequestVoteReply struct {
// 	Term int
// 	VoteGranted bool

// Your data here (2A).
// }
