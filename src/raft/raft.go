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

	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
	applyCh chan ApplyMsg

	serverState ServerState

	votesReceived int

	currentTerm int     // 需要持久化
	votedFor    int     // 需要持久化
	log         []Entry // 需要持久化
	commitIndex int     // 初始时为 0
	lastApplied int     // 已经应用到状态机的index
	nextIndex   []int
	matchIndex  []int

	reportTaskCh chan func() // 用于保证 ApplyMsg 的顺序，不直接往applyCh里投递消息是因为这玩意会阻塞

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
	rf.persister.Save(rf.encodeRaftState(), rf.persister.ReadSnapshot())
}

func (rf *Raft) encodeRaftState() []byte {
	CurrentTerm := rf.currentTerm
	VotedFor := rf.votedFor
	Logs := rf.log
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(CurrentTerm)
	e.Encode(VotedFor)
	e.Encode(Logs)
	return w.Bytes()
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Logs []Entry
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Logs) != nil {
		DPrintf("[warning] deser failed\n")
	} else {
		rf.currentTerm = CurrentTerm
		rf.votedFor = VotedFor
		rf.log = Logs
		DPrintf("[%v] read: %v %v %v\n", rf.me, CurrentTerm, VotedFor, Logs)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Snapshot called index:%v me:%v\n", index, rf.me)
	rf.printInner()
	currentSnapshotIdx := rf.log[0].Index
	if index <= currentSnapshotIdx {
		return
	}

	j := rf.findEntryPosByIndex(index)
	if j == -1 {
		DPrintf("cannot find index\n")
		return
	}
	rf.log = rf.log[j:]
	rf.log[0].Command = nil
	rf.persister.Save(rf.encodeRaftState(), snapshot)
	// TODO: 确认这里是否可以直接增加commitIndex
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
}

// not thread-safe
func (rf *Raft) becomeFollower(newTerm int) {
	rf.serverState = Follower
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm // TODO: persist
		rf.votedFor = -1         // TODO: persist
		// 这里就算是更新了term与votedFor但恰好卡在这里崩溃没有持久化也是ok的，
		// 持久化的主要意义在于，确定这个term下的选举投票情况。恰逢follower更新
		// term时挂了，节点重启时重新走流程就好，因为选票还没有给任何其他节点
		rf.persist()
	}

	t := RandomizedElectionTime()
	rf.electionTimer.Reset(t)
	rf.heartbeatTimer.Stop()

	DPrintf("follower=%v term=%v %v\n", rf.me, rf.currentTerm, t)
	rf.printInner()
}

func (rf *Raft) recoverFromPersist() {
	DPrintf("%v recovered term=%v grant=%v\n", rf.me, rf.currentTerm, rf.votedFor)
	t := RandomizedElectionTime()
	rf.electionTimer.Reset(t)
	rf.heartbeatTimer.Stop()
}

func (rf *Raft) printLeader() {
	DPrintf("match:%v next:%v\n", rf.matchIndex, rf.nextIndex)
}

// not thread-safe
func (rf *Raft) becomeCandidate() {
	rf.serverState = Candidate
	rf.currentTerm++     // TODO: persist
	rf.votedFor = rf.me  // TODO: persist
	rf.votesReceived = 1 // vote for itself
	DPrintf("%v grants %v in term=%v\n", rf.me, rf.votedFor, rf.currentTerm)
	rf.persist()
	lastLog := rf.GetLastLog()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(term, voteFor, sendTo, lastLogIdx, lastLogTerm int) {
			// request for votes
			request := RequestVoteArgs{Term: term, CandidateId: voteFor, LastLogIndex: lastLogIdx, LastLogTerm: lastLogTerm}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(sendTo, &request, &reply) {
				rf.OnReceiveVoteReply(&reply)
			} else {
				DPrintf("connection failed %v<->%v", voteFor, sendTo)
			}
		}(rf.currentTerm, rf.me, i, lastLog.Index, lastLog.Term)
	}

	t := RandomizedElectionTime()
	rf.electionTimer.Reset(t)
	rf.heartbeatTimer.Stop()

	DPrintf("candidate=%v term=%v %v\n", rf.me, rf.currentTerm, t)
	rf.printInner()
}

// not thread-safe
func (rf *Raft) becomeLeader() {
	rf.serverState = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	rf.heartbeatTimerExpiredImpl() // immediately notify all other peers

	rf.heartbeatTimer.Reset(RandomizedHeartbeatTime())
	rf.electionTimer.Stop()

	DPrintf("leader=%v term=%v votes=%v\n", rf.me, rf.currentTerm, rf.votesReceived)
	rf.printInner()
	rf.printLeader()
}

// example RequestVote RPC handler.
// receive Request from other peers.
// thread-safe
func (rf *Raft) OnReceiveRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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
	lastLog := rf.GetLastLog()
	DPrintf("term:(%v,%v) log_idx:(%v,%v)\n", args.LastLogTerm, lastLog.Term, args.LastLogIndex, lastLog.Index)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLog.Term || args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index) {
		// log新与旧怎么判断？
		// 先看term，term相同的话，再看id
		rf.votedFor = args.CandidateId // TODO: persist
		rf.persist()
		reply.VoteGranted = true
		DPrintf("%v grants %v in term=%v\n", rf.me, rf.votedFor, rf.currentTerm)
	} else {
		reply.VoteGranted = false
	}
}

// not thread safe
func (rf *Raft) GetLastLog() Entry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) PrintState() {
	DPrintf("[%v] commit:%v log:%v \n", rf.me, rf.commitIndex, rf.log)
	if rf.serverState == Leader {
		DPrintf("[%v:%v] match:%v next:%v\n", rf.me, rf.currentTerm, rf.matchIndex, rf.nextIndex)
	}
}

// thread-safe
// receive AppendEntries from current leader
func (rf *Raft) OnReceiveAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) > 0 {
		DPrintf("%v recv AE idx:[%v,%v]\n", rf.me, args.Entries[0].Index, args.Entries[len(args.Entries)-1].Index)
	} else {
		DPrintf("%v recv AE empty\n", rf.me)
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.becomeFollower(args.Term)

	// TODO: 如果 rf.log 中有 dummy 数值的话，重新考虑下这里的逻辑
	idx := rf.findEntryPosByIndex(args.PrevLogIndex)

	if idx == -1 || args.PrevLogTerm != rf.log[idx].Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// overwrite
	rf.log = append(rf.log[:idx+1], args.Entries...)
	DPrintf("%v last log:%v\n", rf.me, rf.GetLastLog())
	rf.printInner()
	// TODO: persist
	rf.persist()

	rf.advanceFollowerCommit(args.LeaderCommit)

	reply.Term = rf.currentTerm
	reply.Success = true
}

// thread unsafe
func (rf *Raft) reportCommit(prevIdx int) {
	start := rf.findEntryPosByIndex(prevIdx) + 1
	// 如果snapshot覆盖了log，完全可能会出现被覆盖的部分之前
	// 没有发送过ApplyMsg。这里该如何处理？暂时先只发送拥有的log吧
	// 注释掉这里的panic
	if start == 0 {
		// panic("cannot find prevIdx")
		start = 1
	}
	j := len(rf.log)
	for i := start; i < len(rf.log); i++ {
		if rf.log[i].Index > rf.commitIndex {
			j = i
			break
		}
	}
	forReport := make([]Entry, j-start)
	DPrintf("%v report commit range:[%v,%v]\n", rf.me, rf.log[start].Index, rf.log[j-1].Index)
	copy(forReport, rf.log[start:j])
	// 这里太坑了，applyCh有可能会被阻塞，所以需要copy一份，然后起一个goroutine来将
	// 已经commit的log发送到applyCh
	rf.reportTaskCh <- func() {
		for i := range forReport {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      forReport[i].Command,
				CommandIndex: forReport[i].Index,
			}
		}
	}

}

func (rf *Raft) advanceFollowerCommit(leaderCommit int) {
	minInt := func(a, b int) int {
		if a < b {
			return a
		} else {
			return b
		}
	}
	prevCommit := rf.commitIndex
	if leaderCommit > rf.commitIndex {
		// make sure len(rf.log) > 0
		// "index of last new entry" 到底指的是啥？
		rf.commitIndex = minInt(leaderCommit, rf.GetLastLog().Index)
	}
	if prevCommit != rf.commitIndex {
		rf.reportCommit(prevCommit)
	}
}

func (rf *Raft) OnReceiveInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v recv IS idx=%v incterm=%v\n", rf.me, args.LastIncludedIndex, args.LastIncludedIndex)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	// args.Term == rf.currentTerm 时，args发送者一定是当前term的leader，
	// 接受者还傻了吧唧是candidate呢，所以应该直接转成该term的follower
	rf.becomeFollower(rf.currentTerm) // 这里注意，只有ServerState改变了，term可能没变，处理逻辑可能稍有不同

	if args.LastIncludedIndex < rf.commitIndex {
		return
	}

	j := rf.findEntryPosByIndex(args.LastIncludedIndex) // 0...j should be discarded
	if j != -1 {
		tmp := rf.log[j+1:]
		rf.log = []Entry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}} // dummy log
		rf.log = append(rf.log, tmp...)
	} else {
		rf.log = []Entry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}} // dummy log
	}
	// TODO: persist
	rf.persister.Save(rf.encodeRaftState(), args.Data) // TODO: becomeFollower那里可能persist了一次，考虑消除一次persist以提高性能
	// TODO: 确认下这里直接更新commitidx可以吗？
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	reply.Term = rf.currentTerm

	args_copy := *args
	DPrintf("%v commit snapshot idx=%v\n", rf.me, args_copy.LastIncludedIndex)
	rf.reportTaskCh <- func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			SnapshotTerm:  args_copy.Term,
			SnapshotIndex: args_copy.LastIncludedIndex,
			Snapshot:      args_copy.Data,
		}
	}
}

func (rf *Raft) PrintInnerForTester() {
	s := ""
	for _, entry := range rf.log {
		s += fmt.Sprintf("{%v %v %v} ", entry.Index, entry.Term, entry.Command)
	}
	state := ""
	switch rf.serverState {
	case Leader:
		state = "ld"
	case Candidate:
		state = "cd"
	case Follower:
		state = "fo"
	}
	fmt.Printf("[%v:%s:(%v)] commit:%v log:%v\n", rf.me, state, rf.currentTerm, rf.commitIndex, s)
}
func (rf *Raft) printInner() {
	s := ""
	for _, entry := range rf.log {
		s += fmt.Sprintf("{%v %v %v} ", entry.Index, entry.Term, entry.Command)
	}
	state := ""
	switch rf.serverState {
	case Leader:
		state = "ld"
	case Candidate:
		state = "cd"
	case Follower:
		state = "fo"
	}
	DPrintf("[%v:%s:(%v)] commit:%v log:%v", rf.me, state, rf.currentTerm, rf.commitIndex, s)
}

func (rf *Raft) OnReceiveInstallSnapshotReply(reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
	}
}

// not thread-safe
func (rf *Raft) BuildAndSendInstallSnapshotRequest(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	go func(request *InstallSnapshotArgs) {
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		if ok {
			rf.OnReceiveInstallSnapshotReply(&reply)
		}
	}(&args)
}

// thread safe
func (rf *Raft) OnReceiveAppendEntriesReply(server int, reply *AppendEntriesReply, lastIdx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	switch rf.serverState {
	case Follower, Candidate:
		// log.Println("[warning] follower or candidate received AE reply")
	case Leader:
		DPrintf("recv AE reply from %v\n", server)
		if reply.Success {
			rf.matchIndex[server] = lastIdx
			rf.nextIndex[server] = lastIdx + 1
			rf.IncreaseCommitIndexForLeader()
		} else {
			// TODO: 到底减去多少，还需要fine-tuning
			rf.nextIndex[server] -= 1
			if rf.nextIndex[server] < rf.log[0].Index+1 {
				rf.nextIndex[server] = rf.log[0].Index + 1
				rf.BuildAndSendInstallSnapshotRequest(server)
			}
			rf.BuildAppendEntriesRequestAndSend(server)
		}
	}

}

// not thread safe
func (rf *Raft) BuildAppendEntriesRequestForFollower(server int) AppendEntriesArgs {
	prev_idx := rf.nextIndex[server] - 1
	k := rf.findEntryPosByIndex(prev_idx)
	if k == -1 {
		panic(fmt.Sprintf("[%v] index=%v not found", rf.me, prev_idx))
	}
	entries := []Entry{}
	for i := k + 1; i < len(rf.log); i++ {
		entries = append(entries, rf.log[i])
	}
	request := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.log[k].Index,
		PrevLogTerm:  rf.log[k].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return request
}

func (rf *Raft) buildHeartbeatRequest() AppendEntriesArgs {
	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.GetLastLog().Index,
		PrevLogTerm:  rf.GetLastLog().Term,
		Entries:      []Entry{},
		LeaderCommit: rf.commitIndex,
	}
}

// not thread safe
func (rf *Raft) BuildAppendEntriesRequestAndSend(to int) {
	if to == rf.me {
		panic("to eq rf.me")
	}
	request := rf.BuildAppendEntriesRequestForFollower(to)
	DPrintf("build AE request:%v\n", request)
	go func(server int, request *AppendEntriesArgs, last int) {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, request, &reply)
		if ok {
			rf.OnReceiveAppendEntriesReply(server, &reply, last)
		}
	}(to, &request, rf.GetLastLog().Index)
}

func (rf *Raft) sendAppendEntriesOrSnapshot(to int) {
	if to == rf.me {
		panic("to eq rf.me")
	}

}

func (rf *Raft) sendAppendEntriesRequest(req AppendEntriesArgs, to int) {
	go func(request *AppendEntriesArgs, last int) {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(to, request, &reply)
		if ok {
			rf.OnReceiveAppendEntriesReply(to, &reply, last)
		}
	}(&req, rf.GetLastLog().Index)
}

// not thread safe
func (rf *Raft) findEntryPosByIndex(index int) int {
	// consider binary search
	firstLogIndex := rf.log[0].Index
	i := index - firstLogIndex
	if i >= 0 && i < len(rf.log) {
		return i
	} else {
		return -1
	}
}

// not thread safe
func (rf *Raft) IncreaseCommitIndexForLeader() {
	if rf.serverState != Leader {
		panic("not leader")
	}
	matchIdxCopy := make([]int, len(rf.matchIndex))
	copy(matchIdxCopy, rf.matchIndex)
	SortIntDesc(matchIdxCopy)
	N := matchIdxCopy[len(matchIdxCopy)/2]
	prevCommit := rf.commitIndex
	if N > rf.commitIndex {
		i := rf.findEntryPosByIndex(N)
		if i != -1 && rf.log[i].Term == rf.currentTerm {
			rf.commitIndex = N
		}
	}
	if prevCommit != rf.commitIndex {
		rf.reportCommit(prevCommit)
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
	ok := rf.peers[server].Call("Raft.OnReceiveRequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.OnReceiveAppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.OnReceiveInstallSnapshot", args, reply)
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
// (index, term, isLeader)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState != Leader {
		return -1, -1, false
	}

	rf.AppendNewLog(command)

	// TODO: notify other servers

	lastLog := rf.GetLastLog()
	DPrintf("append log {%v %v}\n", lastLog.Index, lastLog.Term)

	return lastLog.Index, lastLog.Term, true
}

// not thread-safe
func (rf *Raft) AppendNewLog(command interface{}) {
	DPrintf("appending...\n")
	lastIdx := rf.log[len(rf.log)-1].Index
	term := rf.currentTerm
	rf.matchIndex[rf.me] = lastIdx + 1
	rf.nextIndex[rf.me] = lastIdx + 2
	rf.log = append(rf.log, Entry{Index: lastIdx + 1, Term: term, Command: command})
	// TODO: persist
	rf.persist()
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
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// thread-safe
func (rf *Raft) OnReceiveVoteReply(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("recv vote %v for %v", reply, rf.me)

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
	} else if reply.Term < rf.currentTerm {
		// 收到的投票不是当前term的，直接丢弃
		return
	} else {
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
}

// thread-safe
func (rf *Raft) electionTimerExpiredImpl() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.serverState {
	case Follower, Candidate:
		rf.becomeCandidate()
	case Leader:
		DPrintf("[warning] %v(leader) triggered by election timer\n", rf.me)
		rf.electionTimer.Stop()
	}
}

// not thread-safe
func (rf *Raft) heartbeatTimerExpiredImpl() {
	switch rf.serverState {
	case Follower, Candidate:
		DPrintf("[warning] %v(candidate or follower) triggered by heartbeat timer\n", rf.me)
		rf.heartbeatTimer.Stop()
	case Leader:
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			// TODO: 考虑下这里是>=还是>
			if rf.nextIndex[i] > rf.log[0].Index {
				rf.BuildAppendEntriesRequestAndSend(i)
			} else {
				// follower 的log 差的太远了，ld都已经snapshot了，直接发送InstallSnapshot请求
				rf.BuildAndSendInstallSnapshotRequest(i)
				// 刷新 follower 的 commitIndex
				req := rf.buildHeartbeatRequest()
				rf.sendAppendEntriesRequest(req, i)
			}
		}
		rf.printInner()
		rf.printLeader()
		rf.heartbeatTimer.Reset(RandomizedHeartbeatTime())
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		// TODO: 看下这里，能不能保证调用*Impl时，一定是处于相应的状态？
		case <-rf.electionTimer.C:
			rf.electionTimerExpiredImpl()
		case <-rf.heartbeatTimer.C:
			// rf.serverState 这里完全有可能是 Follower 或者 Candidate。
			// 假设定时器channel在raft是ld时触发，但进入该分支时，rf被锁住了，
			// 同时还从ld变成了follower 或者 candidate。若此处再获取到mutex，
			// rf就的确不是ld了
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
	DPrintf("starting %v\n", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.applyCh = applyCh
	rf.reportTaskCh = make(chan func(), 1000)
	go func() {
		for f := range rf.reportTaskCh {
			f()
		}
	}()

	rf.log = make([]Entry, 1)
	rf.currentTerm = -1 // not initialized
	rf.commitIndex = 0

	rf.mu = sync.Mutex{}
	rf.electionTimer = time.NewTimer(100 * time.Second)  // sleep by default
	rf.heartbeatTimer = time.NewTimer(100 * time.Second) // sleep by default

	// initialize from state persisted before a crash
	if persister.RaftStateSize() == 0 {
		rf.becomeFollower(0)
	} else {
		rf.readPersist(persister.ReadRaftState())
		rf.recoverFromPersist()
	}
	rf.becomeFollower(0) // must be after readPersist

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
