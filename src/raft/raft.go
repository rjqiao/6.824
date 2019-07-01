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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

const (
	Follower = iota
	Candidate
	Leader
)

func init() {
	//rand.Seed(time.Now().UTC().UnixNano())
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent on all servers
	currentTerm int
	votedFor    int
	logs        [] LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// others
	status int

	applyCh            chan<- ApplyMsg
	gotGoodRequestVote chan bool
	heartbeat          chan bool
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// RaftPersistence is persisted to the `persister`, and contains all necessary data to restart a failed node
type RaftPersistence struct {
	CurrentTerm int
	Logs        []LogEntry
	VotedFor    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.status == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	buf := new(bytes.Buffer)
	_ = gob.NewEncoder(buf).Encode(
		RaftPersistence{
			CurrentTerm: rf.currentTerm,
			Logs:        rf.logs,
			VotedFor:    rf.votedFor,
		})

	RaftDebug("Persisting node data (%d bytes)", rf, buf.Len())
	rf.persister.SaveRaftState(buf.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	d := gob.NewDecoder(r)
	obj := RaftPersistence{}
	_ = d.Decode(&obj)

	rf.votedFor, rf.currentTerm, rf.logs = obj.VotedFor, obj.CurrentTerm, obj.Logs
	RaftInfo("Loaded persisted node data (%d bytes). Last applied index: %d", rf, len(data), rf.lastApplied)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	SuggestIndex int // next try for PrevLogIndex
	SuggestTerm  int
	ConflictTerm int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) transitionToCandidate() {
	rf.status = Candidate
	// Increment currentTerm and vote for self
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) transitionToFollower(newTerm int) {
	rf.status = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

// lock outside
// assert rf.status != Leader
func (rf *Raft) promoteToLeader() {
	if rf.status == Leader {
		return
	}

	rf.status = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = rf.getLastIndex() + 1 // Should be initialized to leader's last log index + 1
			rf.matchIndex[i] = 0                    // Index of highest log entry known to be replicated on server
		}
	}
	go rf.heartbeatDaemonProcess()
}

func (rf *Raft) setCommitIndexAndApplyStateMachine(commitIndex int) {
	rf.commitIndex = commitIndex;
	go rf.applyLocalStateMachine()
}

func (rf *Raft) isUptoDate(cIndex int, cTerm int) bool {
	term, index := rf.getLastTerm(), rf.getLastIndex()
	if cTerm == term {
		return cIndex >= index
	} else {
		return cTerm >= term
	}
}

// assert 0 <= index <= rf.getLastIndex()
func (rf *Raft) getTermForIndex(index int) int {
	if index == 0 {
		return 0
	} else {
		return rf.logs[index-1].Term
	}
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs)
}

func (rf *Raft) getLastTerm() int {
	if len(rf.logs) == 0 {
		return 0
	}
	return rf.logs[len(rf.logs)-1].Term
}

// lock outside, call with go
// 什么改变时候会updateCommitIndex呢
// modify rf.commitIndex
// send to rf.applyCh
func (rf *Raft) updateCommitIndex() {
	index := len(rf.logs)
	oldCommitIndex := rf.commitIndex
	for index > oldCommitIndex {
		entry := rf.logs[index-1]
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= entry.Index {
				count++
			}
		}
		if count*2 > len(rf.peers) {
			// 必须是一个当前term的、成为majority的log
			if entry.Term >= rf.currentTerm {
				rf.setCommitIndexAndApplyStateMachine(entry.Index)
				break
			}
		}
		index--
	}
}

// send to rf.applyCh
func (rf *Raft) applyLocalStateMachine() {
	rf.mu.Lock()
	if rf.commitIndex > 0 && rf.commitIndex > rf.lastApplied {
		entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
		RaftDebug("Applying: len(rf.logs) = %d", rf, len(rf.logs))
		copy(entries, rf.logs[rf.lastApplied:rf.commitIndex])
		rf.mu.Unlock()
		RaftInfo("Locally applying %d log entries. lastApplied: %d. commitIndex: %d", rf, len(entries), rf.lastApplied, rf.commitIndex)
		for _, log := range entries {
			rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: log.Index, Command: log.Command}
		}

		rf.mu.Lock()
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateLogAndCommitIndexWhenReceivingAppendEntriesSuccess(Entries []LogEntry, PrevLogIndex int, LeaderCommit int) {
	if len(Entries) != 0 {
		lastValidIndex := PrevLogIndex
		for lastValidIndex+1 <= MinInt(Entries[len(Entries)-1].Index, rf.getLastIndex()) &&
			rf.getTermForIndex(lastValidIndex+1) == Entries[lastValidIndex-PrevLogIndex].Term {
			lastValidIndex++
		}
		rf.logs = append(rf.logs[:lastValidIndex], Entries[lastValidIndex-PrevLogIndex:]...)
		//rf.persist()
	}

	// update commitIndex
	// assert args.LeaderCommit <= rf.getLastEntryIndex()
	// assert LeaderCommit >= rf.commitIndex

	// can be old message? without MaxInt
	rf.setCommitIndexAndApplyStateMachine(MaxInt(LeaderCommit, rf.commitIndex))
}

// lock outside
// no side effect
func (rf *Raft) buildAppendEntriesReplyWhenNotSuccess(reply *AppendEntriesReply, PrevLogIndex int, PrevLogTerm int) {
	if PrevLogIndex > rf.getLastIndex() {
		reply.ConflictTerm = -1
		reply.SuggestIndex = rf.getLastIndex()
		reply.SuggestTerm = rf.getLastTerm()
	} else {
		reply.ConflictTerm = rf.getTermForIndex(PrevLogIndex)
		if reply.ConflictTerm > PrevLogTerm {
			// suggestTerm = the max index ( <= PrevLogTerm )
			reply.SuggestIndex = PrevLogIndex
			for ; reply.SuggestIndex >= 1 && rf.getTermForIndex(reply.SuggestIndex) > PrevLogTerm; reply.SuggestIndex-- {
			}
			reply.SuggestTerm = rf.getTermForIndex(reply.SuggestIndex) // term 0 if index 0
		} else {
			// assert reply.ConflictTerm < args.PrevLogTerm
			reply.SuggestIndex = PrevLogIndex - 1
			reply.SuggestTerm = rf.getTermForIndex(reply.SuggestIndex) // term 0 if index 0
		}
	}
}

// lock inside
// no side effect
func (rf *Raft) buildAppendEntriesArgs(server int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLogIndex := 0
	prevLogTerm := 0
	var entries []LogEntry
	leaderCommit := rf.commitIndex

	// assert nextIndex[server]>=1  因为index 0的entry不存在
	// rf.getLastIndex >= 0
	if rf.nextIndex[server] > rf.getLastIndex() {
		entries = make([]LogEntry, 0)
		prevLogIndex = rf.getLastIndex()
		prevLogTerm = rf.getLastTerm()
	} else {
		entries = rf.logs[(rf.nextIndex[server] - 1):]
		prevLogIndex = rf.nextIndex[server] - 1
		// assert rf.nextIndex[server] >= 1
		if rf.nextIndex[server] <= 1 {
			prevLogTerm = 0
		} else {
			prevLogTerm = rf.logs[(rf.nextIndex[server] - 2)].Term
		}
	}

	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
}

// lock outside
// side effect
func (rf *Raft) updateNextIndexWhenAppendEntriesFail(server int, reply *AppendEntriesReply) {
	//lastTryIndex := rf.nextIndex[server]
	nextPrevLogIndex := 1
	if rf.getTermForIndex(reply.SuggestIndex) == reply.SuggestTerm {
		// including index==0 && term==0
		nextPrevLogIndex = reply.SuggestIndex
	} else if rf.getTermForIndex(reply.SuggestIndex) > reply.SuggestTerm {
		npi := reply.SuggestIndex
		for ; npi >= 1 && rf.getTermForIndex(npi) > reply.SuggestTerm; npi-- {
		}
		// side effect
		nextPrevLogIndex = npi
	} else {
		// assert reply.SuggestIndex >= 1
		// side effect
		nextPrevLogIndex = reply.SuggestIndex - 1
	}

	// side effect
	rf.nextIndex[server] = nextPrevLogIndex + 1
	// assert 1 <= rf.nextIndex[server] <= rf.getLastIndex() + 1

	// not needed
	//rf.nextIndex[server] = MaxInt(MinInt(rf.nextIndex[server], lastTryIndex-1), 1)
}

// side effect
// 更新nextIndex, matchIndex, updatecommitIndex
// send to applyCh
// lock outside
func (rf *Raft) updateIndexesAndApplyWhenSuccess(server int) {
	rf.nextIndex[server] = rf.getLastIndex() + 1
	rf.matchIndex[server] = rf.getLastIndex()
	rf.updateCommitIndex()
	RaftDebug("Send AppendEntries to %d ++: new matchIndex = %d, commitIndex = %d", rf, server, rf.matchIndex[server], rf.commitIndex)
}

// ----------------------------------------------------------------

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.transitionToFollower(args.Term)
	}

	rf.gotGoodRequestVote <- true
	// assert(args.Term == rf.currentTerm)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUptoDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = args.Term
	} else {
		reply.VoteGranted = false
		reply.Term = args.Term
	}

	rf.persist()
}

//
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
//

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	requestBlock := func() bool { return rf.peers[server].Call("Raft.RequestVote", args, reply) }
	ok := sendRPCRequest("Raft.RequestVote", requestBlock)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//RaftDebug("AppendEntries: leader = %d", rf, args.LeaderId)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		// no need -- reply.NextTryIndex
		return
	}

	rf.heartbeat <- true

	if args.Term > rf.currentTerm || rf.status == Candidate {
		rf.transitionToFollower(args.Term)
	}

	RaftDebug("AppendEntries: args.LeaderCommit = %d", rf, args.LeaderCommit)

	reply.Term = rf.currentTerm

	// check PrevLogIndex and PrevLogTerm
	if (args.PrevLogIndex == 0) ||
		(args.PrevLogIndex <= rf.getLastIndex() && args.PrevLogTerm == rf.getTermForIndex(args.PrevLogIndex)) {
		// no conflict
		reply.Success = true
		rf.updateLogAndCommitIndexWhenReceivingAppendEntriesSuccess(args.Entries, args.PrevLogIndex, args.LeaderCommit)
	} else {
		// assert PrevLogIndex >= 1 && PrevLogTerm >= 1
		reply.Success = false
		rf.buildAppendEntriesReplyWhenNotSuccess(reply, args.PrevLogIndex, args.PrevLogTerm)
	}
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	requestBlock := func() bool {
		return rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	ok := sendRPCRequest("Raft.AppendEntries", requestBlock)
	return ok
}

func (rf *Raft) sendAndCollectAppendEntries(server int) bool {
	args := rf.buildAppendEntriesArgs(server) // lock inside

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return ok
	}

	RaftDebug("Send AppendEntries to %d ++: reply = %v", rf, server, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 上锁之后检查consistency
	// 会出现不是leader的情况吗？
	// 检查stale leader，应该不需要
	if rf.status != Leader || args.Term < rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.transitionToFollower(reply.Term)
		rf.persist()
		return ok
	}

	//RaftDebug("append entries reply success? %v, reply term %d, from server %d", rf, reply.Success, reply.Term, server)

	// side effect
	if reply.Success {
		// todo: if entries是空 -- 这里是为什么是todo？
		rf.updateIndexesAndApplyWhenSuccess(server)
	} else {
		rf.updateNextIndexWhenAppendEntriesFail(server, reply)
	}

	return ok
}

func (rf *Raft) sendAllAppendEntries() {
	for i := range rf.peers {
		rf.mu.Lock()
		if i != rf.me && rf.status == Leader {
			rf.mu.Unlock()
			go rf.sendAndCollectAppendEntries(i)
		} else {
			rf.mu.Unlock()
		}
	}
	//rf.persist()
}

func (rf *Raft) heartbeatDaemonProcess() {
	RaftDebug("heartbeat daemon started\n", rf)
	var heartbeatTimeout = time.Millisecond * 120

	for {
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			rf.sendAllAppendEntries()
		} else {
			rf.mu.Unlock()
			return
		}
		// 后sleep！！！
		time.Sleep(heartbeatTimeout)
	}
}

func (rf *Raft) electionDaemonProcess() {
	electionTimeout := func() time.Duration {
		t := time.Duration(rand.Intn(300)) + 200
		return time.Millisecond * (t)
	}
	for {
		currTimeout := electionTimeout()
		select {
		// 重置timeout
		case <-rf.heartbeat:
		case <-rf.gotGoodRequestVote:

		case <-time.After(currTimeout):
			// 需不需要其他条件？比如时间
			if rf.status != Leader {
				go rf.doElection()
			}
		}
	}
}

func (rf *Raft) doElection() {
	rf.mu.Lock()
	RaftInfo("election timeout! start election\n", rf)
	rf.transitionToCandidate()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastTerm(),
		LastLogIndex: rf.getLastIndex(),
	}

	rf.persist() // because transiting to Candidate, currentTerm++
	rf.mu.Unlock()

	var voteCount = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			RaftDebug("do Election %d -> %d, start term %d", rf, rf.me, i, args.Term)
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			RaftDebug("do Election reply %d -> %d, start-term %d, reply : %v", rf, rf.me, i, args.Term, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 是不是不需要？
			// Follower 说明有人能new
			// Leader 说明已经成为Leader，结束
			// stale candidate
			if rf.status != Candidate || rf.currentTerm != args.Term {
				return
			}

			// 是不是不需要？
			// 其他人的term更加新
			if reply.Term > rf.currentTerm {
				rf.transitionToFollower(reply.Term)
				rf.persist() // change term
				return
			}

			// 确认consistent了
			if reply.VoteGranted {
				voteCount++
				//atomic.AddInt32(&voteCount, 1)
			}

			if voteCount*2 > len(rf.peers) && args.Term == rf.currentTerm && rf.status == Candidate {
				rf.promoteToLeader()
				RaftInfo("become leader", rf)
			}
		}(i)
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.status == Leader

	// Your code here (2B).

	if !isLeader {
		return index, term, isLeader
	}
	index = rf.getLastIndex() + 1
	rf.logs = append(rf.logs, LogEntry{Index: index, Term: term, Command: command})
	rf.persist()
	RaftInfo("New entry appended to leader's log: %v", rf, rf.logs[index-1])

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.transitionToFollower(0)
	rf.applyCh = applyCh
	rf.gotGoodRequestVote = make(chan bool, 100)
	rf.heartbeat = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionDaemonProcess()

	RaftInfo("Started server", rf)
	return rf
}
