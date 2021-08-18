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

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
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

	Snapshot []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent on all servers
	currentTerm int
	votedFor    int // -1 when this node is Follower and do not know who is leader and did not vote for leader
	logs        []LogEntry
	// Persist for snapshot
	snapshotIndex int
	snapshotTerm  int

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders

	// next start of Entries in AppendEntries
	// what about snapshot?
	nextIndex  []int
	matchIndex []int

	// others
	status int

	applyCh chan<- ApplyMsg

	electionTimerReset chan bool
	heartbeatTimerEnd  chan bool

	killCh chan bool

	ApplyCond *sync.Cond

	// debug only
	nanoSecCreated int64
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

	SnapshotIndex int
	SnapshotTerm  int
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
	rf.persistRaftStateAndSnapshot(nil)
}

func (rf *Raft) persistRaftStateAndSnapshot(snapshot []byte) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	raftPersistence := RaftPersistence{
		CurrentTerm: rf.currentTerm,
		Logs:        rf.logs,
		VotedFor:    rf.votedFor,

		SnapshotIndex: rf.snapshotIndex,
		SnapshotTerm:  rf.snapshotTerm,
	}

	buf := new(bytes.Buffer)
	_ = gob.NewEncoder(buf).Encode(raftPersistence)

	RaftDebug("Persisting node data", rf)
	RaftTrace("Persisting node data, %v", rf, raftPersistence)

	if len(snapshot) < 1 {
		rf.Persister.SaveRaftState(buf.Bytes())
	} else {
		rf.Persister.SaveStateAndSnapshot(buf.Bytes(), snapshot)
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// TODO: remove $data argument?
	// Handle empty $data
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

	rf.votedFor, rf.currentTerm, rf.logs, rf.snapshotIndex, rf.snapshotTerm =
		obj.VotedFor, obj.CurrentTerm, obj.Logs, obj.SnapshotIndex, obj.SnapshotTerm
	RaftDebug("Loaded persisted node data (%d bytes). Last applied index: %d", rf, len(data), rf.lastApplied)
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
	Term                int
	Success             bool
	SuggestPrevLogIndex int // next try for PrevLogIndex
	SuggestPrevLogTerm  int
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// will update -- logs, commitIndex, snapshotIndex, snapshotTerm,
func (rf *Raft) PersistSnapshotAndDiscardLogs(lastIncludedSnapshotIndex int, snapShotBytes []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedSnapshotIndex <= rf.snapshotIndex {
		RaftInfo("appliedIndex {%d} fall behind snapshotIndex {%d}", rf, lastIncludedSnapshotIndex, rf.snapshotIndex)
		return
	}

	RaftInfo("lastIncludedSnapshotIndex = %d, rf.snapshotIndex = %d", rf, lastIncludedSnapshotIndex, rf.snapshotIndex)
	AssertF(lastIncludedSnapshotIndex <= rf.commitIndex, "lastIncludedSnapshotIndex=%d, rf.commitIndex=%d", lastIncludedSnapshotIndex, rf.commitIndex)

	AssertF(rf.commitIndex >= lastIncludedSnapshotIndex,
		"rf.commitIndex {%d} >= lastIncludedSnapshotIndex {%d}",
		rf.commitIndex, lastIncludedSnapshotIndex)

	rf.persistSnapshotAndDiscardLogsInner(lastIncludedSnapshotIndex, rf.getTermForIndex(lastIncludedSnapshotIndex), snapShotBytes)
}

// Locked outside
func (rf *Raft) persistSnapshotAndDiscardLogsInner(lastIncludedSnapshotIndex int, lastIncludedSnapshotTerm int, snapShotBytes []byte) {

	// 1. When raft receive InstallSnapshot, if conflict, then discard all state, and follow leader,
	//    else only accept and remove its own log before lastIncludedSnapshot
	// 2. When raft receive PersistSnapshot from kvraft, it should not discard new logs after $lastIncludedSnapshotIndex
	// --->
	AssertF(lastIncludedSnapshotIndex > 0, "lastIncludedSnapshotIndex {%d} > 0 Failed!", lastIncludedSnapshotIndex)
	if rf.snapshotIndex >= lastIncludedSnapshotIndex {
		return
	}

	AssertF(lastIncludedSnapshotIndex > rf.commitIndex || rf.getTermForIndex(lastIncludedSnapshotIndex) == lastIncludedSnapshotTerm, "")

	if lastIncludedSnapshotIndex > rf.getLastIndex() || lastIncludedSnapshotTerm != rf.getTermForIndex(lastIncludedSnapshotIndex) {
		// remove any logs or snapshot in this raft, reset hard to leader
		rf.logs = nil
	} else {
		rf.logs = rf.logs[rf.getOffsetFromIndex(lastIncludedSnapshotIndex)+1:]
	}

	rf.snapshotTerm = lastIncludedSnapshotTerm
	rf.snapshotIndex = lastIncludedSnapshotIndex

	PanicIfF(len(snapShotBytes) < 1, "snapShot len == 0")
	rf.persistRaftStateAndSnapshot(snapShotBytes)
	rf.setCommitIndexAndApplyStateMachine(rf.snapshotIndex)
}

func (rf *Raft) transitionToCandidate() {
	rf.status = Candidate
	// Increment currentTerm and vote for self
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) transitionToFollower(newTerm int, votedFor int) {
	RaftDebug("transit to follower, new term: %d", rf, newTerm)
	AssertF(newTerm >= rf.currentTerm,
		"newTerm {%d} >=rf.currentTerm {%d}",
		newTerm, rf.currentTerm)
	rf.status = Follower
	rf.currentTerm = newTerm
	rf.votedFor = votedFor
}

// lock outside
func (rf *Raft) promoteToLeader() {
	PanicIfF(rf.status == Leader, "Should not become leader when already leader")

	rf.status = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = rf.getLastIndex() + 1 // Should be initialized to leader's last log index + 1
			rf.matchIndex[i] = 0                    // Index of highest log entry known to be replicated on server
		}
	}

	// append a ControlCommand LeaderBroadcast
	//go rf.Start(LeaderBroadcast{})
}

// locked outside
func (rf *Raft) setCommitIndexAndApplyStateMachine(commitIndex int) {
	// commitIndex monotonically increases
	if rf.commitIndex < commitIndex {
		AssertF(commitIndex >= rf.snapshotIndex,
			"commitIndex {%d} >= rf.snapshotIndex {%d}",
			commitIndex, rf.snapshotIndex)
		rf.commitIndex = commitIndex
		RaftInfo("Commit to commitIndex: %d", rf, commitIndex)
		rf.ApplyCond.Broadcast()
	}
}

func (rf *Raft) isUptoDate(cIndex int, cTerm int) bool {
	term, index := rf.getLastTerm(), rf.getLastIndex()
	if cTerm == term {
		return cIndex >= index
	}
	return cTerm >= term
}

func (rf *Raft) getOffsetFromIndex(index int) int {
	AssertF(index >= rf.snapshotIndex,
		"index {%d} >= rf.snapshotIndex {%d}", index, rf.snapshotIndex)
	return index - rf.snapshotIndex - 1
}

// assert rf.snapshotIndex <= index <= rf.getLastIndex()
func (rf *Raft) getTermForIndex(index int) int {
	PanicIfF(index < rf.snapshotIndex, "getTermForIndex: index to small: index=%d, snapshotIndex=%d", index, rf.snapshotIndex)
	PanicIfF(index > rf.getLastIndex(), "index > rf.getLastIndex()")

	if index == rf.snapshotIndex {
		return rf.snapshotTerm
	}
	offset := rf.getOffsetFromIndex(index)
	PanicIfF(offset >= len(rf.logs), "offset{%d} >= len(rf.logs){%d}", offset, len(rf.logs))
	return rf.logs[offset].Term
}

func (rf *Raft) getLastIndex() int {
	return rf.snapshotIndex + len(rf.logs)
}

func (rf *Raft) getLastTerm() int {
	if len(rf.logs) == 0 {
		return rf.snapshotTerm
	}
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) updateCommitIndex() {
	index := rf.getLastIndex()
	AssertF(rf.commitIndex >= rf.snapshotIndex, "rf.commitIndex=%d, rf.snapshotIndex=%d", rf.commitIndex, rf.snapshotIndex)
	for index > rf.commitIndex {
		entry := rf.logs[rf.getOffsetFromIndex(index)]
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= entry.Index {
				count++
			}
		}
		if count*2 > len(rf.peers) {
			// see a log in this term
			if entry.Term >= rf.currentTerm {
				rf.setCommitIndexAndApplyStateMachine(entry.Index)
				break
			}
		}
		index--
	}
}

func (rf *Raft) updateLogAndCommitIndexWhenReceivingAppendEntriesSuccess(Entries []LogEntry, PrevLogIndex, LeaderCommit int) {
	if len(Entries) != 0 {
		getOffsetInEntries := func(i int) int {
			return i - PrevLogIndex - 1
		}

		index := MaxInt(PrevLogIndex+1, rf.snapshotIndex+1)
		AssertF(getOffsetInEntries(index) >= 0, "getOffsetInEntries(index) {%d}", getOffsetInEntries(index))
		for ; index <= rf.commitIndex && getOffsetInEntries(index) < len(Entries); index++ {
			AssertF(Entries[getOffsetInEntries(index)].Index == index, "")
			AssertF(rf.getTermForIndex(index) == Entries[getOffsetInEntries(index)].Term, "")
		}

		for ; index <= rf.getLastIndex() && getOffsetInEntries(index) < len(Entries) && rf.getTermForIndex(index) == Entries[getOffsetInEntries(index)].Term; index++ {
			AssertF(Entries[getOffsetInEntries(index)].Index == index, "")
		}

		if getOffsetInEntries(index) < len(Entries) {
			// conflict
			// or Entries has longer logs
			AssertF(index <= rf.getLastIndex()+1, "")
			AssertF(index <= rf.snapshotIndex+1 || rf.logs[rf.getOffsetFromIndex(index-1)].Index == index-1, "")
			AssertF(Entries[getOffsetInEntries(index)].Index == index, "")
			rf.logs = append(rf.logs[:rf.getOffsetFromIndex(index)], Entries[getOffsetInEntries(index):]...)
		} else {
			// rf.logs longer (or equal), no conflict
		}

		rf.setCommitIndexAndApplyStateMachine(MinInt(LeaderCommit, Entries[len(Entries)-1].Index))
	} else {
		rf.setCommitIndexAndApplyStateMachine(LeaderCommit)
	}
}

// lock outside
// no side effect
func (rf *Raft) buildAppendEntriesReplyWhenNotSuccess(reply *AppendEntriesReply, PrevLogIndex int, PrevLogTerm int) {
	if PrevLogIndex > rf.getLastIndex() {
		// this raft do not know about the PrevLogIndex
		reply.SuggestPrevLogIndex = rf.getLastIndex()
		reply.SuggestPrevLogTerm = rf.getLastTerm()
	} else {
		// there is conflict!
		ConflictTerm := rf.getTermForIndex(PrevLogIndex)
		AssertF(ConflictTerm != PrevLogTerm, "")
		AssertF(PrevLogIndex > rf.commitIndex, "")

		// TODO: change to (ConflictTerm, FirstIndex)
		if ConflictTerm > PrevLogTerm {
			// T1 -- PrevLogTerm, T2 -- ConflictTerm, T1<T2
			// any (i1,t1) in leaders log, if i1<=PrevLogIndex, then t1<=PrevLogTerm
			// Then we find SuggestPrevLogIndex, in tuple (SuggestPrevLogIndex, t2),
			// that satisfies t2<=T1, and SuggestPrevLogIndex is the large one
			// suggestTerm = the max index ( <= PrevLogTerm )
			reply.SuggestPrevLogIndex = PrevLogIndex
			for ; reply.SuggestPrevLogIndex > rf.commitIndex && rf.getTermForIndex(reply.SuggestPrevLogIndex) > PrevLogTerm; reply.SuggestPrevLogIndex-- {
			}
			reply.SuggestPrevLogTerm = rf.getTermForIndex(reply.SuggestPrevLogIndex) // term 0 if index 0
		} else {
			reply.SuggestPrevLogIndex = PrevLogIndex - 1
			reply.SuggestPrevLogTerm = rf.getTermForIndex(reply.SuggestPrevLogIndex) // term 0 if index 0
		}

		AssertF(reply.SuggestPrevLogIndex >= rf.commitIndex,
			"reply.SuggestPrevLogIndex {%d} >= rf.commitIndex {%d}",
			reply.SuggestPrevLogIndex, rf.commitIndex)
	}
	AssertF(reply.SuggestPrevLogIndex < PrevLogIndex,
		"reply.SuggestPrevLogIndex {%d} < PrevLogIndex {%d}",
		reply.SuggestPrevLogIndex, PrevLogIndex)
}

// lock outside
// side effect
func (rf *Raft) updateNextIndexWhenAppendEntriesFail(server int, reply *AppendEntriesReply) {
	//lastTryIndex := rf.nextIndex[server]
	if reply.SuggestPrevLogIndex < rf.snapshotIndex {
		// suggestPrevLogIndex+1 is the one that should be the first entry in AppendEntries
		// If suggestPrevLogIndex+1 <= rf.snapshotIndex, then we cannot find the entry

		// the next time will send snapshotIndex
		// including index==0 && term==0 when rf.snapshotIndex>0 ?
		rf.nextIndex[server] = rf.snapshotIndex
	} else if rf.getTermForIndex(reply.SuggestPrevLogIndex) == reply.SuggestPrevLogTerm {
		// including index==0 && term==0 when rf.snapshotIndex==0 ?
		rf.nextIndex[server] = reply.SuggestPrevLogIndex + 1
	} else if rf.getTermForIndex(reply.SuggestPrevLogIndex) > reply.SuggestPrevLogTerm {
		npi := reply.SuggestPrevLogIndex
		for ; npi >= rf.snapshotIndex+1 && rf.getTermForIndex(npi) > reply.SuggestPrevLogTerm; npi-- {
		}
		rf.nextIndex[server] = npi + 1
	} else {
		AssertF(reply.SuggestPrevLogIndex >= rf.snapshotIndex+1,
			"reply.SuggestPrevLogIndex {%d} >= rf.snapshotIndex+1 {%d}",
			reply.SuggestPrevLogIndex, rf.snapshotIndex+1)
		rf.nextIndex[server] = reply.SuggestPrevLogIndex
	}

	RaftDebug("SendAppendEntries failed to %d ++: rf.nextIndex[%d]=%d",
		rf, server, server, rf.nextIndex[server])

	AssertF(rf.nextIndex[server] >= rf.snapshotIndex && rf.nextIndex[server] <= rf.getLastIndex()+1, "")

}

// side effect
// 更新nextIndex, matchIndex, updatecommitIndex
// send to applyCh
// lock outside
func (rf *Raft) updateIndexesAndApplyWhenSuccess(server int, args *AppendEntriesArgs) {
	if len(args.Entries) == 0 {
		RaftDebug("Heartbeat success to %d", rf, server)
		return
	}

	AssertF(args.Entries[len(args.Entries)-1].Index == args.PrevLogIndex+len(args.Entries),
		"args.Entries[len(args.Entries)-1].Index {%d} == args.PrevLogIndex {%d} + len(args.Entries) {%d}",
		args.Entries[len(args.Entries)-1].Index, args.PrevLogIndex, len(args.Entries))

	// Only use $args but not current rf!!!!
	lastIndexNewlyAppendToServer := args.PrevLogIndex + len(args.Entries)

	rf.nextIndex[server] = lastIndexNewlyAppendToServer + 1
	rf.matchIndex[server] = MaxInt(rf.matchIndex[server], lastIndexNewlyAppendToServer)

	rf.updateCommitIndex()

	RaftDebug("SendAppendEntries succeeded to %d ++: new matchIndex = %d, commitIndex = %d",
		rf, server, rf.matchIndex[server], rf.commitIndex)
}

// ----------------------------------------------------------------

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO: fail this rpc when killed

	// Your code here (2A, 2B).
	isGoodRequestVote := false
	rf.mu.Lock()

	defer func() {
		AssertF(reply.Term >= args.Term, "reply.Term {%d} >= args.Term {%d}", reply.Term, args.Term)
		rf.mu.Unlock()
		rf.resetElectionTimerIf(isGoodRequestVote)
	}()

	if args.Term < rf.currentTerm {
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		return
	}

	if args.Term > rf.currentTerm {
		rf.transitionToFollower(args.Term, -1)
	}

	AssertF(args.Term == rf.currentTerm, "")

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUptoDate(args.LastLogIndex, args.LastLogTerm) {
		isGoodRequestVote = true
		rf.votedFor = args.CandidateId
		*reply = RequestVoteReply{Term: args.Term, VoteGranted: true}
	} else {
		*reply = RequestVoteReply{Term: args.Term, VoteGranted: false}
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
	ok := SendRPCRequestWithRetry("Raft.RequestVote", RaftRPCTimeout, 3, requestBlock)
	return ok
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	requestBlock := func() bool { return rf.peers[server].Call("Raft.InstallSnapshot", args, reply) }
	ok := SendRPCRequestWithRetry("Raft.InstallSnapshot", RaftRPCTimeout, 1, requestBlock)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// TODO: fail this rpc when killed

	goodHeartBeat := false
	rf.mu.Lock()
	defer func() {
		AssertF(reply.Term >= args.Term, "reply.Term {%d} >= args.Term {%d}", reply.Term, args.Term)
		rf.mu.Unlock()
		rf.resetElectionTimerIf(goodHeartBeat)
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	AssertF(rf.status != Leader || rf.currentTerm < args.Term, "")

	goodHeartBeat = true

	if args.Term > rf.currentTerm || rf.votedFor != args.LeaderId {
		rf.transitionToFollower(args.Term, args.LeaderId)
	}

	AssertF(rf.status == Follower && rf.currentTerm == rf.currentTerm && rf.votedFor == args.LeaderId, "")

	reply.Term = rf.currentTerm

	RaftDebug("InstallSnapshot, LeaderId=%d, lastIncludedIndex=%d, lastIncludedTerm=%d", rf, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)

	rf.persistSnapshotAndDiscardLogsInner(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: fail this rpc when killed

	goodHeartBeat := false
	rf.mu.Lock()
	defer func() {
		AssertF(reply.Term >= args.Term, "reply.Term {%d} >= args.Term {%d}", reply.Term, args.Term)
		rf.mu.Unlock()
		rf.resetElectionTimerIf(goodHeartBeat)
	}()

	// check --- currentTerm, status, votedFor

	// rf term ahead --- return false
	// rf term behind --- reset election timer
	// should not see rf is leader (assert term is same)
	// rf is not follower --- reset election timer
	// votedFor not LeaderId --- reset election timer
	// transition to follower, go ahead

	// args.Term --- rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	AssertF(rf.status != Leader || args.Term > rf.currentTerm, "")

	goodHeartBeat = true

	// correct follower state following
	if args.Term > rf.currentTerm || rf.votedFor != args.LeaderId {
		rf.transitionToFollower(args.Term, args.LeaderId)
	}

	AssertF(rf.status == Follower && rf.currentTerm == args.Term && rf.votedFor == args.LeaderId, "")

	RaftDebug("AppendEntries: args.LeaderCommit = %d", rf, args.LeaderCommit)

	reply.Term = rf.currentTerm

	if args.PrevLogIndex < rf.commitIndex && (len(args.Entries) == 0 ||
		args.Entries[len(args.Entries)-1].Index < rf.commitIndex) {
		// very old RPC call
		// do not update anything
		reply.Success = true
		return
	}

	if args.PrevLogIndex <= rf.commitIndex && args.PrevLogIndex >= rf.snapshotIndex {
		AssertF(args.PrevLogTerm == rf.getTermForIndex(args.PrevLogIndex),
			"args.PrevLogTerm {%d} == rf.getTermForIndex(args.PrevLogIndex) {%d}",
			args.PrevLogTerm, rf.getTermForIndex(args.PrevLogIndex))
	}

	if args.PrevLogIndex+1 <= rf.commitIndex &&
		len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Index >= rf.commitIndex {

		AssertF(args.PrevLogIndex+1 == args.Entries[0].Index,
			"args.PrevLogIndex+1 {%d} == args.Entries[0].Index {%d}",
			args.PrevLogIndex+1, args.Entries[0].Index)

		entry := args.Entries[rf.commitIndex-args.PrevLogIndex-1]
		AssertF(entry.Index == rf.commitIndex,
			"entry.Index {%d} == rf.commitIndex {%d}",
			entry.Index, rf.commitIndex)
		AssertF(entry.Term == rf.getTermForIndex(rf.commitIndex),
			"entry.Term {%d} == rf.getTermForIndex(rf.commitIndex) {%d}",
			entry.Term, rf.getTermForIndex(rf.commitIndex))
	}

	// check PrevLogIndex and PrevLogTerm
	// Make sure that this server know about PrevLogIndex,
	// (PrevLogIndex, PrevLogTerm) pair matches this raft
	// (PrevLogIndex, PrevLogTerm) == (0,0) is included
	if args.PrevLogIndex <= rf.getLastIndex() &&
		(args.PrevLogIndex <= rf.commitIndex || args.PrevLogTerm == rf.getTermForIndex(args.PrevLogIndex)) {
		// no conflict
		reply.Success = true
		rf.updateLogAndCommitIndexWhenReceivingAppendEntriesSuccess(args.Entries, args.PrevLogIndex, args.LeaderCommit)
	} else {
		// assert PrevLogIndex >= 1 && PrevLogTerm >= 1
		reply.Success = false
		rf.buildAppendEntriesReplyWhenNotSuccess(reply, args.PrevLogIndex, args.PrevLogTerm)
	}

	if len(rf.logs) != 0 {
		AssertF(rf.logs[len(rf.logs)-1].Index >= rf.commitIndex,
			"rf.logs[len(rf.logs)-1].Index {%d} >= rf.commitIndex {%d}",
			rf.logs[len(rf.logs)-1].Index, rf.commitIndex)
	}

	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	requestBlock := func() bool { return rf.peers[server].Call("Raft.AppendEntries", args, reply) }
	ok := SendRPCRequestWithRetry("Raft.AppendEntries", RaftRPCTimeout, 1, requestBlock)
	return ok
}

func (rf *Raft) beforeSendAppendEntries(server int) *AppendEntriesArgs {
	AssertF(rf.status == Leader, "")

	prevLogIndex := 0
	prevLogTerm := 0
	var entries []LogEntry
	leaderCommit := rf.commitIndex

	AssertF(rf.nextIndex[server] >= 1,
		"rf.nextIndex[server] {%d} >= 1", rf.nextIndex[server])
	// 因为index 0的entry不存在
	// rf.getLastIndex >= 0
	if rf.nextIndex[server] > rf.getLastIndex() {
		AssertF(rf.nextIndex[server] == rf.getLastIndex()+1,
			"rf.nextIndex[server] {%v} == rf.getLastIndex()+1 {%v}",
			rf.nextIndex[server], rf.getLastIndex()+1)
		// Heartbeat
		entries = make([]LogEntry, 0)
		prevLogIndex = rf.getLastIndex()
		prevLogTerm = rf.getLastTerm()
	} else {
		offset := rf.getOffsetFromIndex(rf.nextIndex[server])
		entriesToCopy := rf.logs[offset:]
		entries = make([]LogEntry, len(entriesToCopy))
		copy(entries, entriesToCopy)

		AssertF(rf.nextIndex[server] > rf.snapshotIndex,
			"rf.nextIndex[server] {%d} > rf.snapshotIndex {%d}",
			rf.nextIndex[server], rf.snapshotIndex)
		prevLogIndex = rf.nextIndex[server] - 1
		prevLogTerm = rf.getTermForIndex(prevLogIndex)

		AssertF(prevLogIndex == entries[0].Index-1,
			"prevLogIndex {%d} == entries[0].Index-1 {%d}",
			prevLogIndex, entries[0].Index-1)
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

func (rf *Raft) resetElectionTimerIf(b bool) {
	if b {
		select {
		case <-rf.killCh:
		case rf.electionTimerReset <- true:
		}
	}
}

func (rf *Raft) afterSendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	isResetElectionTimer := false
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		rf.resetElectionTimerIf(isResetElectionTimer)
	}()

	RaftDebug("SendAppendEntries to %d ++: reply = %v", rf, server, reply)

	AssertF(reply.Term >= args.Term,
		"reply.Term {%d} >= args.Term {%d}",
		reply.Term, args.Term)
	AssertF(rf.currentTerm >= args.Term,
		"rf.currentTerm {%d} >= args.Term {%d}",
		rf.currentTerm, args.Term)

	if rf.currentTerm < reply.Term {
		rf.transitionToFollower(reply.Term, server)
		rf.persist()
		isResetElectionTimer = true
		return false
	}

	// should not work on stale RPC
	if args.Term != rf.currentTerm || rf.currentTerm > reply.Term {
		return false
	}

	AssertF(rf.currentTerm == args.Term && rf.currentTerm == reply.Term && rf.status == Leader, "")

	RaftTrace("append entries reply success? %v, reply term %d, from server %d", rf, reply.Success, reply.Term, server)

	if reply.Success {
		rf.updateIndexesAndApplyWhenSuccess(server, args)
	} else {
		rf.updateNextIndexWhenAppendEntriesFail(server, reply)
		// trigger SendAppendEntries when last time failed
		rf.sendOneAppendEntriesOrInstallSnapshot(server)
	}

	return true
}

func (rf *Raft) sendAndCollectAppendEntries(server int) {
	args := rf.beforeSendAppendEntries(server)
	AssertF(args != nil, "")

	RaftDebug("SendAppendEntries to %d --", rf, server)

	go func() {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, args, reply)
		if !ok {
			return
		}
		rf.afterSendAppendEntries(server, args, reply)
	}()
}

func (rf *Raft) afterSendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	isResetElectionTimer := false
	defer func() {
		rf.mu.Unlock()
		rf.resetElectionTimerIf(isResetElectionTimer)
	}()

	RaftDebug("SendInstallSnapshot to %d ++: reply = %v", rf, server, reply)

	AssertF(reply.Term >= args.Term, "")
	AssertF(rf.currentTerm >= args.Term, "")

	if rf.currentTerm < reply.Term {
		rf.transitionToFollower(reply.Term, server)
		rf.persist()
		isResetElectionTimer = true
		return false
	}

	// should not work on stale RPC
	if args.Term != rf.currentTerm || rf.currentTerm > reply.Term {
		return false
	}

	AssertF(rf.currentTerm == args.Term && rf.currentTerm == reply.Term && rf.status == Leader, "")

	// update nextIndex and matchIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = MaxInt(rf.matchIndex[server], args.LastIncludedIndex)
	rf.updateCommitIndex()

	return true
}

func (rf *Raft) sendAndCollectInstallSnapshot(server int) {
	AssertF(rf.status == Leader, "")
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Data:              rf.Persister.ReadSnapshot(),
	}
	RaftDebug("SendInstallSnapshot to %d --", rf, server)

	go func() {
		reply := &InstallSnapshotReply{}
		ok := rf.sendInstallSnapShot(server, args, reply)
		if !ok {
			return
		}
		rf.afterSendInstallSnapshot(server, args, reply)
	}()
}

// No lock inside, should add lock outside
// do not block
func (rf *Raft) sendAllAppendEntriesOrInstallSnapshot() {
	AssertF(rf.commitIndex >= rf.snapshotIndex,
		"rf.commitIndex {%d} >= rf.snapshotIndex {%d}",
		rf.commitIndex, rf.snapshotIndex)

	if rf.status != Leader {
		return
	}

	for i := range rf.peers {
		if i != rf.me {
			rf.sendOneAppendEntriesOrInstallSnapshot(i)
		}
	}
}

func (rf *Raft) sendOneAppendEntriesOrInstallSnapshot(server int) {
	AssertF(server != rf.me, "")

	if rf.snapshotIndex < rf.nextIndex[server] {
		rf.sendAndCollectAppendEntries(server)
	} else {
		rf.sendAndCollectInstallSnapshot(server)
	}
}

func (rf *Raft) heartbeatDaemonProcess() {
	RaftDebug("heartbeat daemon started\n", rf)

	for {
		select {
		case <-rf.killCh:
			return
		default:
		}

		rf.mu.Lock()
		CallWhenRepeatNTimes(10, func() { RaftTrace("heartbeat!\n", rf) })()
		rf.sendAllAppendEntriesOrInstallSnapshot()
		rf.mu.Unlock()
		// sleep later
		time.Sleep(HeartbeatTimeout)
	}
}

func (rf *Raft) electionDaemonProcess() {
	electionTimeout := func() time.Duration {
		return electionBaseTimeout + time.Duration(rand.Int63n(int64(electionRandomTimeout)))
	}
	for {
		currTimeout := electionTimeout()
		select {
		case <-rf.killCh:
			return
		case <-time.After(currTimeout):
			go rf.doElection()
		case <-rf.electionTimerReset:
		}
	}
}

func (rf *Raft) doElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == Leader {
		return
	}

	RaftDebug("Election timeout! start election\n", rf)
	rf.transitionToCandidate()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastTerm(),
		LastLogIndex: rf.getLastIndex(),
	}

	rf.persist() // because transiting to Candidate, currentTerm++

	var voteCount = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			//RaftTrace("do Election %d -> %d, start term %d", rf, rf.me, i, args.Term)
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			//RaftTrace("do Election reply %d -> %d, start-term %d, reply : %v", rf, rf.me, i, args.Term, reply)
			if !ok {
				return
			}

			isResetElectionTimer := false
			rf.mu.Lock()
			defer func() {
				rf.mu.Unlock()
				rf.resetElectionTimerIf(isResetElectionTimer)
			}()

			AssertF(args.Term <= reply.Term, "args.Term {%d} <= reply.Term {%d}", args.Term, reply.Term)
			AssertF(args.Term <= rf.currentTerm, "args.Term {%d} <= rf.currentTerm {%d}", args.Term, rf.currentTerm)

			// see newer term
			if rf.currentTerm < reply.Term {
				rf.transitionToFollower(reply.Term, i)
				rf.persist() // change term
				isResetElectionTimer = true
				return
			}

			// stale reply, or already leader
			if rf.currentTerm != args.Term || rf.status != Candidate {
				return
			}

			AssertF(args.Term == reply.Term && args.Term == rf.currentTerm && rf.status == Candidate, "")

			if reply.VoteGranted {
				voteCount++
			}

			if voteCount*2 > len(rf.peers) {
				rf.promoteToLeader()
				RaftDebug("Become leader", rf)
			}
		}(i)
	}
}

func (rf *Raft) applyDaemonProcess() {
	for {
		rf.mu.Lock()
		AssertF(rf.commitIndex >= rf.lastApplied,
			"rf.commitIndex {%d} >= rf.lastApplied {%d} Failed!",
			rf.commitIndex, rf.lastApplied)

		RaftTrace("rf.commitIndex {%d}, rf.lastApplied {%d}",
			rf, rf.commitIndex, rf.lastApplied)

		for rf.commitIndex == rf.lastApplied {
			rf.ApplyCond.Wait()
			select {
			case <-rf.killCh:
				// do we close rf.applyCh?
				//close(rf.applyCh)
				rf.mu.Unlock()
				return
			default:
			}
		}

		RaftInfo("rf.commitIndex {%d}, rf.lastApplied {%d}",
			rf, rf.commitIndex, rf.lastApplied)
		AssertF(rf.commitIndex > rf.lastApplied,
			"rf.commitIndex {%d} > rf.lastApplied {%d} Failed!",
			rf.commitIndex, rf.lastApplied)
		AssertF(rf.commitIndex > 0, "commit index should > 0")
		AssertF(rf.commitIndex >= rf.snapshotIndex, "rf.commitIndex {%d} >= rf.snapshotIndex {%d} Failed!", rf.commitIndex, rf.snapshotIndex)

		// 1. raft reboot
		// 2. receive InstallSnapshot
		// When kvraft triggers a snapshot, we do not need to re-install snapshot back to kvraft
		// check if we should install snapshot to kvraft
		if rf.lastApplied < rf.snapshotIndex {
			RaftInfo("Apply Install Snapshot, snapshotIndex=%d, lastApplied=%d", rf, rf.snapshotIndex, rf.lastApplied)
			// rf.lastApplied < rf.snapshotIndex <= rf.commitIndex
			rf.lastApplied = rf.snapshotIndex
			rf.mu.Unlock()
			select {
			case <-rf.killCh:
				return
			case rf.applyCh <- ApplyMsg{
				CommandValid: false,
				Command:      nil,
				CommandIndex: 0,
				Snapshot:     rf.Persister.ReadSnapshot(),
			}:
			}
			// do not wait, since snapshot might not be the latest
		} else {
			// rf.snapshotIndex <= rf.lastApplied < rf.commitIndex
			entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
			RaftTrace("Applying: len(rf.logs) = %d, snapshotIndex=%d", rf, len(rf.logs), rf.snapshotIndex)
			copy(entries, rf.logs[rf.getOffsetFromIndex(rf.lastApplied)+1:rf.getOffsetFromIndex(rf.commitIndex)+1])

			AssertF(rf.lastApplied+1 == entries[0].Index, "apply not in order!")
			rf.lastApplied = rf.commitIndex

			RaftInfo("Apply! len(entries)=%d", rf, len(entries))

			rf.mu.Unlock()

			for _, log0 := range entries {
				select {
				case <-rf.killCh:
					// do we close rf.applyCh?
					//close(rf.applyCh)
					return
				case rf.applyCh <- ApplyMsg{
					CommandValid: true,
					CommandIndex: log0.Index,
					Command:      log0.Command,
					Snapshot:     nil}:
				}
			}

			time.Sleep(applyTimeout)
		}
	}
}

func (rf *Raft) periodicDump() {
	for {
		select {
		case <-rf.killCh:
			return
		default:
		}

		time.Sleep(500 * time.Millisecond)
		rf.mu.Lock()

		var logs []LogEntry
		if len(rf.logs) >= 10 {
			logs = rf.logs[len(rf.logs)-10:]
		} else {
			logs = rf.logs
		}

		RaftInfo("[DUMP] snapShotIndex=%d, snapshotTerm=%d, commitIndex=%d, lastApplied=%d, currentTerm=%d, vodedFor=%d, status=%d, nextIndex=%v, matchIndex=%v, len(rf.logs)=%d, rf.Persister.RaftStateSize()=%d, rf.logs[-10:]=%v",
			rf, rf.snapshotIndex, rf.snapshotTerm, rf.commitIndex, rf.lastApplied, rf.currentTerm, rf.votedFor, rf.status, rf.nextIndex, rf.matchIndex, len(rf.logs), rf.Persister.RaftStateSize(), logs)

		rf.mu.Unlock()
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
	rf.sendAllAppendEntriesOrInstallSnapshot() // broadcast new log to followers
	rf.persist()
	AssertF(rf.getOffsetFromIndex(index) < len(rf.logs), "index=%d, len(rf.logs)=%d, rf.snapshotIndex=%d", index, len(rf.logs), rf.snapshotIndex)
	RaftInfo("New entry appended to leader's log: %v", rf, rf.logs[rf.getOffsetFromIndex(index)])

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
	rf.mu.Lock()
	RaftDebug("Killed!", rf)
	rf.mu.Unlock()

	close(rf.killCh)
	//close(rf.applyCh)
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
	rf.Persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.transitionToFollower(0, -1)
	rf.applyCh = applyCh

	rf.electionTimerReset = make(chan bool, 100)

	rf.killCh = make(chan bool)
	rf.ApplyCond = sync.NewCond(&rf.mu)

	// init persist fields (only for no raftState persist)
	// it should be ok not to init, but for consistency
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.setCommitIndexAndApplyStateMachine(rf.snapshotIndex)
	PanicIfF(rf.snapshotIndex > rf.commitIndex, "should not happen")
	AssertF(rf.snapshotIndex <= rf.commitIndex,
		"rf.snapshotIndex {%d} <= rf.commitIndex {%d}",
		rf.snapshotIndex, rf.commitIndex)

	RaftDebug("Started server", rf)

	// debug only
	rf.nanoSecCreated = time.Now().UnixNano()

	go rf.electionDaemonProcess()
	go rf.applyDaemonProcess()
	go rf.periodicDump()
	go rf.heartbeatDaemonProcess()

	return rf
}
