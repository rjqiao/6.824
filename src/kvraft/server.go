package kvraft

import (
	"bytes"
	"encoding/gob"
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

type Operation int

type KVServer struct {
	// if we want to get kv.rf.mu, first get kv.mu
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// View of log, will eventually consistent to raft logs
	data                  map[string]string          // Key -> Value
	notifyChs             map[int]chan raft.ApplyMsg // logIndex -> channel
	latestRequests        map[int64]RaftKVCommand    // ClerkId -> ReqSeq
	latestAppliedLogIndex int                        // logIndex last applied

	killCh chan bool
}

type SnapShotPersistence struct {
	Data                  map[string]string
	LatestAppliedLogIndex int
	LatestRequests        map[int64]RaftKVCommand
}

func (kv *KVServer) needSnapShot() bool {
	if kv.maxraftstate < 0 {
		return false
	}
	KVServerInfo("kv.rf.Persister.RaftStateSize()=%d, kv.maxraftstate=%d", kv, kv.rf.Persister.RaftStateSize(), kv.maxraftstate)
	if kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
		return true
	}
	diff := kv.maxraftstate - kv.rf.Persister.RaftStateSize()
	threshold := kv.maxraftstate / 10
	return diff < threshold
}

// should hold lock outside
func (kv *KVServer) generateSnapshotAndApplyToRaft() {
	snapShotPersistence := SnapShotPersistence{
		Data:                  kv.data,
		LatestAppliedLogIndex: kv.latestAppliedLogIndex,
		LatestRequests:        kv.latestRequests,
	}
	buf := new(bytes.Buffer)
	_ = gob.NewEncoder(buf).Encode(snapShotPersistence)

	kv.rf.PersistSnapshotAndDiscardLogs(kv.latestAppliedLogIndex, buf.Bytes())
}

func (kv *KVServer) applySnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	snapShotPersistence := SnapShotPersistence{}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	_ = d.Decode(&snapShotPersistence)

	kv.data = snapShotPersistence.Data
	KVServerInfo("latestAppliedLogIndex=%d, snapShotPersistence.LatestAppliedLogIndex=%d",kv, kv.latestAppliedLogIndex, snapShotPersistence.LatestAppliedLogIndex)
	kv.latestAppliedLogIndex = snapShotPersistence.LatestAppliedLogIndex
	kv.latestRequests = snapShotPersistence.LatestRequests

	//kv.snapshotIndex = snapShotPersistence.SnapShotIndex
}

func (kv *KVServer) handleApplyMsg() {
	for {
		select {
		case <-kv.killCh:
			return
		case msg, ok := <-kv.applyCh:
			if !ok {
				return
			}
			if !msg.CommandValid {
				kv.mu.Lock()
				// got a snapshot
				raft.PanicIfF(len(msg.Snapshot)==0, "")
				kv.applySnapshot(msg.Snapshot)
				kv.mu.Unlock()
				continue
			}

			// Is it here to change view of state machine? Yes
			command := msg.Command.(RaftKVCommand)

			kv.mu.Lock()

			KVServerDebug("get from ApplyCh, ClerkId: %d, ReqSeq: %d, Op: %s, Key: %s, Value: %s, CommitIndex: %d", kv,
				command.ClerkId, command.RequestSeq, command.Op, command.Key, command.Value, msg.CommandIndex)

			raft.AssertF(kv.latestAppliedLogIndex < msg.CommandIndex, "should not see obsolete apply")
			raft.AssertF(kv.latestAppliedLogIndex == msg.CommandIndex-1,
				"kv.latestAppliedLogIndex{%d} == msg.CommandIndex {%d} -1 -- Failed!", kv.latestAppliedLogIndex, msg.CommandIndex)

			raft.PanicIfF(kv.latestAppliedLogIndex != msg.CommandIndex-1,
				"not in order. kv.latestAppliedLogIndex=%d, msg.CommandIndex=%d",
				kv.latestAppliedLogIndex, msg.CommandIndex)


			if lastCommand, ok := kv.latestRequests[command.ClerkId]; !ok || lastCommand.RequestSeq != command.RequestSeq {
				switch command.Op {
				case "Put":
					kv.data[command.Key] = command.Value
				case "Append":
					kv.data[command.Key] += command.Value // What happen when nil?
				}
				command.Value = kv.data[command.Key] // case "Get"
				KVServerDebug("In db (non dup) -- Op: %s, Key: %s, Value: %s", kv, command.Op, command.Key, command.Value)
				kv.latestRequests[command.ClerkId] = command
			} else {
				raft.PanicIfF(lastCommand.Key != command.Key,
					"Key should be same, ClerkId: %d, ReqSeq: %d, Op: %s, Key: %s, Value: %s, CommitIndex: %d, ",
					command.ClerkId, command.RequestSeq, command.Op, command.Key, command.Value, msg.CommandIndex)
				command.Value = lastCommand.Value
				KVServerDebug("In db (dup) -- Op: %s, Key: %s, Value: %s", kv, command.Op, command.Key, command.Value)
			}
			msg.Command = command
			kv.latestAppliedLogIndex = msg.CommandIndex


			// check and generate snapshot (after new applied)
			if kv.needSnapShot() {
				// might not be able to take snapshot (latestAppliedLogIndex <= snapshotIndex can happen)
				// because KV state is not always consistent with Raft state
				kv.generateSnapshotAndApplyToRaft()
			}

			// !ok
			// 1. Not leader, so no kv.notifyChs[msg.CommandIndex] created
			// 2. stale or short cut request ??
			if ch, ok := kv.notifyChs[msg.CommandIndex]; ok {
				delete(kv.notifyChs, msg.CommandIndex)

				go func() {
					timeout := 500 * time.Millisecond
					select {
					case ch <- msg:
					case <-time.After(timeout):
					}
					// producer close the channel
					close(ch)
				}()
			} else {
				KVServerDebug("get from ApplyCh, channel failure", kv)
			}
			kv.mu.Unlock()

		}
	}
}

// timeout in KvServerWaitNotifyChTimeout
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	command := RaftKVCommand{
		Op:    "Get",
		Key:   args.Key,
		Value: "",

		ClerkId:    args.ClerkId,
		RequestSeq: args.RequestSeq,
	}

	kv.mu.Lock()

	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		*reply = GetReply{WrongLeader: true, Err: ErrWrongLeader, Value: ""}
		kv.mu.Unlock()
		return
	}

	// delete and close old channel, interrupt (old lastIndex) RPC and then return with error (maybe DeprecateReq)
	if _, ok := kv.notifyChs[index]; ok {
		// only remove $ch from notifyChs map, but do not close it.
		// $ch will finally be closed in producer side
		delete(kv.notifyChs, index)
	}

	kv.notifyChs[index] = make(chan raft.ApplyMsg)
	ch := kv.notifyChs[index]

	KVServerDebug("Get received: index -- %d, key: %x", kv, index, args.Key)
	kv.mu.Unlock()

	timer := time.After(KvServerWaitNotifyChTimeout)
	for {
		select {
		// what if
		case msg, ok := <-ch:
			if !ok {
				KVServerDebug("channel has been deleted in Get", kv)
				*reply = GetReply{WrongLeader: false, Err: ErrStaleIndex}
				return
			}

			notifiedCommand := msg.Command.(RaftKVCommand)
			// make sure $command (receive from $ch) is the same (reqSeq, clerkId) one
			if notifiedCommand.RequestSeq != command.RequestSeq || notifiedCommand.ClerkId != command.ClerkId {
				//panic("should not happen! The command received should have same (reqSeq, clerkId) pair")

				// stale channel, early finish
				KVServerDebug("command has different (reqSeq, clerkId) pair, (%d,%d)!=(%d,%d)",
					kv, notifiedCommand.RequestSeq, notifiedCommand.ClerkId, command.RequestSeq, command.ClerkId)
				*reply = GetReply{WrongLeader: false, Err: ErrStaleIndex}
				return
			}

			KVServerDebug("Got reply -- Op: %s, ClerkId: %d. ReqSeq: %d, Key: %x, Value: %x", kv,
				notifiedCommand.Op, notifiedCommand.ClerkId, notifiedCommand.RequestSeq, notifiedCommand.Key, notifiedCommand.Value)

			*reply = GetReply{WrongLeader: false, Err: OK, Value: notifiedCommand.Value}
			return

		case <-time.After(CheckIsLeaderTimeout):
			if !kv.getCheckIsLeader(reply, index) {
				*reply = GetReply{WrongLeader: true, Err: ErrWrongLeader, Value: ""}
				return
			}
		case <-timer:
			kv.mu.Lock()
			if ch0, ok := kv.notifyChs[index]; ok && ch0 == ch {
				delete(kv.notifyChs, index)
			}
			kv.mu.Unlock()
			*reply = GetReply{WrongLeader: false, Err: ErrUnknown, Value: ""}
			return
		}
	}
}

func (kv *KVServer) getCheckIsLeader(reply *GetReply, index int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		*reply = GetReply{WrongLeader: true, Err: ErrWrongLeader}
		delete(kv.notifyChs, index)
		return false
	}
	return true
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := "Put"
	if args.Op == "Append" {
		op = "Append"
	}

	command := RaftKVCommand{
		Op:    op,
		Key:   args.Key,
		Value: args.Value,

		ClerkId:    args.ClerkId,
		RequestSeq: args.RequestSeq,
	}

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)

	KVServerDebug("%s received: index -- %d, key: %s, value: %s, isLeader: %t", kv, op, index, args.Key, args.Value, isLeader)

	if !isLeader {
		kv.mu.Unlock()
		*reply = PutAppendReply{WrongLeader: true, Err: ErrWrongLeader}
		return
	}

	if _, ok := kv.notifyChs[index]; ok {
		// only remove $ch from notifyChs map, but do not close it.
		// $ch will finally be closed in producer side
		delete(kv.notifyChs, index)
	}

	kv.notifyChs[index] = make(chan raft.ApplyMsg)
	ch := kv.notifyChs[index]

	KVServerDebug("%s received: index -- %d, key: %x, value: %x", kv, op, index, args.Key, args.Value)
	kv.mu.Unlock()

	timer := time.After(KvServerWaitNotifyChTimeout)

	for {
		select {
		// TODO: add timeout when waiting $ch result
		// may not receive from $ch, just block here.
		case msg, ok := <-ch:
			if !ok {
				KVServerDebug("channel has been deleted in PutAppend", kv)
				*reply = PutAppendReply{WrongLeader: false, Err: ErrStaleIndex}
				return
			}

			notifiedCommand := msg.Command.(RaftKVCommand)

			KVServerDebug("Got reply -- Op: %s, ClerkId: %d. ReqSeq: %d, Key: %s, Value: %s, CommitIndex: %d", kv,
				notifiedCommand.Op, notifiedCommand.ClerkId, notifiedCommand.RequestSeq, notifiedCommand.Key, notifiedCommand.Value, msg.CommandIndex)

			// make sure $command (receive from $ch) is the same (reqSeq, clerkId) one
			if notifiedCommand.RequestSeq != command.RequestSeq || notifiedCommand.ClerkId != command.ClerkId {
				//panic("should not happen! The command received should have same (reqSeq, clerkId) pair")
				//*reply = PutAppendReply{WrongLeader: true, Err: ErrUnknown}
				//return

				// stale channel, early finish
				KVServerDebug("command has different (reqSeq, clerkId) pair, (%d,%d)!=(%d,%d)",
					kv, notifiedCommand.RequestSeq, notifiedCommand.ClerkId, command.RequestSeq, command.ClerkId)
				*reply = PutAppendReply{WrongLeader: false, Err: ErrStaleIndex}
				return
			}

			*reply = PutAppendReply{WrongLeader: false, Err: OK}
			KVServerDebug("PutAppend RPC Return!", kv)
			return

		// KV Server and Raft in same process (goroutine) and crash at same time
		case <-time.After(CheckIsLeaderTimeout):
			if !kv.putAppendCheckIsLeader(reply, index) {
				*reply = PutAppendReply{WrongLeader: true, Err: ErrWrongLeader}
				return
			}
		case <-timer:
			kv.mu.Lock()
			if ch0, ok := kv.notifyChs[index]; ok && ch0 == ch {
				delete(kv.notifyChs, index)
			}
			kv.mu.Unlock()
			*reply = PutAppendReply{WrongLeader: false, Err: ErrUnknown}
			return
		}

	}
}

func (kv *KVServer) putAppendCheckIsLeader(reply *PutAppendReply, index int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		*reply = PutAppendReply{WrongLeader: true, Err: ErrWrongLeader}
		delete(kv.notifyChs, index)
		return false
	}
	return true
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	// Your code here, if desired.
	close(kv.killCh)

	kv.mu.Lock()
	kv.rf.Kill()
	KVServerDebug("Killed!", kv)
	defer kv.mu.Unlock()

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(RaftKVCommand{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.data = make(map[string]string)
	kv.notifyChs = make(map[int]chan raft.ApplyMsg)
	kv.latestRequests = make(map[int64]RaftKVCommand)
	kv.latestAppliedLogIndex = 0

	//kv.snapshotIndex = 0

	kv.killCh = make(chan bool)

	// You may need initialization code here.

	// should not applySnapshot at start of KV server
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.handleApplyMsg()
	return kv
}
