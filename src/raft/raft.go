package raft

//
// this is an outline of the API that raft must expose to
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
	//	"bytes"

	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type ServerState int

const (
	Follower  ServerState = 0
	Candidate ServerState = 1
	Leader    ServerState = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	serverState ServerState

	votesReceived int

	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer // follower->candidate 以及 candidate->candidate 状态转换的计时器
	heartbeatTimer *time.Timer // 成为 Leader 后，定期向所有其他 peers 发送心跳请求
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.serverState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// not thread-safe
func (rf *Raft) becomeFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.serverState = Follower
	rf.votedFor = -1
	rf.votesReceived = 0

	t := RandomizedElectionTime()
	rf.electionTimer.Reset(t)
	rf.heartbeatTimer.Stop()

	//log.Printf("follower=%v term=%v %v\n", rf.me, rf.currentTerm, t)
}

// not thread-safe
func (rf *Raft) becomeCandidate() {
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesReceived = 1 // vote for itself
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(term, voteFor, sendTo int) {
			// request for votes
			request := RequestVoteArgs{Term: term, CandidateId: voteFor, LastLogIndex: -1, LastLogTerm: -1}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(sendTo, &request, &reply) {
				rf.receiveVoteReply(&reply)
			} else {
				log.Printf("connection failed %v<->%v", voteFor, sendTo)
			}
		}(rf.currentTerm, rf.me, i)
	}

	t := RandomizedElectionTime()
	rf.electionTimer.Reset(t)
	rf.heartbeatTimer.Stop()

	log.Printf("candidate=%v term=%v %v\n", rf.me, rf.currentTerm, t)
}

// not thread-safe
func (rf *Raft) becomeLeader() {
	rf.serverState = Leader

	rf.heartbeatTimerExpiredImpl() // immediately notify all other peers

	rf.heartbeatTimer.Reset(RandomizedHeartbeatTime())
	rf.electionTimer.Stop()

	log.Printf("leader=%v term=%v votes=%v\n", rf.me, rf.currentTerm, rf.votesReceived)
}

// example RequestVote RPC handler.
// receive Request from other peers.
// thread-safe
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = args.Term
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

// thread-safe
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.becomeFollower(args.Term)

	reply.Term = rf.currentTerm
	reply.Success = true

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// thread-safe
func (rf *Raft) receiveVoteReply(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("recv vote %v for %v", reply, rf.me)
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Term < rf.currentTerm {
		return
	}

	switch rf.serverState {
	case Leader, Follower:
		// do nothing
	case Candidate:
		if reply.VoteGranted && reply.Term == rf.currentTerm {
			rf.votesReceived++
			if rf.votesReceived > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}
	}
}

// thread-safe
func (rf *Raft) electionTimerExpiredImpl() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.serverState {
	case Follower, Candidate:
		rf.becomeCandidate()
	case Leader:
		log.Printf("%v(leader) triggered by election timer\n", rf.me)
		rf.electionTimer.Stop()
	}
}

// not thread-safe
func (rf *Raft) heartbeatTimerExpiredImpl() {
	switch rf.serverState {
	case Follower, Candidate:
		log.Printf("%v(candidate or follower) triggered by heartbeat timer\n", rf.me)
		rf.heartbeatTimer.Stop()
	case Leader:
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(term, leader, to int) {
				args := AppendEntriesArgs{Term: term, LeaderId: leader}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(to, &args, &reply)
				if !ok {
					log.Printf("connection failed %v<->%v", leader, to)
				}
				rf.mu.Lock()
				if ok && reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
				rf.mu.Unlock()
			}(rf.currentTerm, rf.me, i)
		}
		rf.heartbeatTimer.Reset(RandomizedHeartbeatTime())
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.electionTimerExpiredImpl()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			rf.heartbeatTimerExpiredImpl()
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.mu = sync.Mutex{}
	rf.electionTimer = time.NewTimer(100 * time.Second)  // sleep by default
	rf.heartbeatTimer = time.NewTimer(100 * time.Second) // sleep by default
	rf.becomeFollower(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
