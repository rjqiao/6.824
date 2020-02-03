package raftkv

import (
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

type Operation int

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// View of log, will eventually consistent to raft logs
	data                  map[string]string          // Key -> Value
	notifyCh              map[int]chan raft.ApplyMsg // logIndex -> channel
	latestRequests        map[int64]int64            // ClerkId -> ReqSeq
	latestAppliedLogIndex int                        // logIndex last applied
}

func (kv *KVServer) handleApplyMsg() {
	for {
		select {
		case msg := <-kv.applyCh:
			// Is it here to change view of state machine? Yes
			command := msg.Command.(RaftKVCommand)

			kv.mu.Lock()

			KVServerInfo("get from ApplyCh, ClerkId: %d, ReqSeq: %d, Op: %s, Key: %s, Value: %s, CommitIndex: %d", kv,
				command.ClerkId, command.RequestSeq, command.Op, command.Key, command.Value, msg.CommandIndex)

			if kv.latestAppliedLogIndex >= msg.CommandIndex {
				KVServerInfo("Obsolete Apply: %d", kv, msg.CommandIndex)
			} else {
				switch command.Op {
				case "Put":
					kv.data[command.Key] = command.Value
				case "Append":
					// 不等幂，需要dedup
					// dedup
					// because each clerk guarantees issue next request after the current request succeeds
					if kv.latestRequests[command.ClerkId] != command.RequestSeq {
						kv.data[command.Key] += command.Value
					}
				case "Get":
					command.Value = kv.data[command.Key]
					msg.Command = command
				default:
				}

				KVServerInfo("In db -- Op: %s, Key: %s, Value: %s", kv, command.Op, command.Key, command.Value)
				kv.latestRequests[command.ClerkId] = command.RequestSeq
			}

			// !ok
			// 1. Not leader, so no kv.notifCh[msg.CommandIndex] created
			// 2. stale or short cut request ??
			if ch, ok := kv.notifyCh[msg.CommandIndex]; ok {
				kv.mu.Unlock()
				ch <- msg
				delete(kv.notifyCh, msg.CommandIndex)
				close(ch)
			} else {
				kv.mu.Unlock()
				KVServerInfo("get from ApplyCh, channel failure", kv)
			}

		}
	}
}

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
		return
	}

	// delete and close old channel, interrupt (old lastIndex) RPC and then return with error (maybe DeprecateReq)
	if _, ok := kv.notifyCh[index]; ok {
		// only remove $ch from notifyCh map, but do not close it.
		// $ch will finally be closed in producer side
		delete(kv.notifyCh, index)
		//close(ch) // should only be deleted in producer side?
	}

	kv.notifyCh[index] = make(chan raft.ApplyMsg)
	ch := kv.notifyCh[index]

	KVServerInfo("Get received: index -- %d, key: %x", kv, index, args.Key)
	kv.mu.Unlock()

	for {
		select {
		// may not receive from $ch, just block here.
		// TODO: add timeout when waiting $ch result
		case msg, ok := <-ch:
			if !ok {
				KVServerInfo("channel has been deleted in Get", kv)
				*reply = GetReply{WrongLeader: false, Err: ErrStaleIndex}
				return
			}

			notifiedCommand := msg.Command.(RaftKVCommand)
			// make sure $command (receive from $ch) is the same (reqSeq, clerkId) one
			if notifiedCommand.RequestSeq != command.RequestSeq || notifiedCommand.ClerkId != command.ClerkId {
				panic("should not happen! The command received should have same (reqSeq, clerkId) pair")
			}

			KVServerInfo("Got reply -- Op: %s, ClerkId: %d. ReqSeq: %d, Key: %x, Value: %x", kv,
				notifiedCommand.Op, notifiedCommand.ClerkId, notifiedCommand.RequestSeq, notifiedCommand.Key, notifiedCommand.Value)

			// might happen !!!  ??
			if notifiedCommand.RequestSeq != command.RequestSeq ||
				notifiedCommand.ClerkId != command.ClerkId {
				panic("should not happen! notifiedCommand != command")

				//*reply = GetReply{WrongLeader: true, Err: ErrUnknown}
				//return
			}
			*reply = GetReply{WrongLeader: false, Err: OK, Value: notifiedCommand.Value,}

			//kv.mu.Lock()
			//delete(kv.notifyCh, index)
			//kv.mu.Unlock()

			return

		case <-time.After(CheckIsLeaderTimeout):
			kv.mu.Lock()
			if _, isLeader := kv.rf.GetState(); !isLeader {
				*reply = GetReply{WrongLeader: true, Err: ErrWrongLeader}
				delete(kv.notifyCh, index)
				kv.mu.Unlock()
				return
			} else {
				kv.mu.Unlock()
			}
		}
	}
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

	KVServerInfo("%s received: index -- %d, key: %s, value: %s, isLeader: %t", kv, op, index, args.Key, args.Value, isLeader)

	if !isLeader {
		*reply = PutAppendReply{WrongLeader: true, Err: ErrWrongLeader}
		kv.mu.Unlock()
		return
	}

	if _, ok := kv.notifyCh[index]; ok {
		// only remove $ch from notifyCh map, but do not close it.
		// $ch will finally be closed in producer side
		delete(kv.notifyCh, index)
		//close(ch)
	}

	kv.notifyCh[index] = make(chan raft.ApplyMsg)
	ch := kv.notifyCh[index]

	KVServerInfo("%s received: index -- %d, key: %x, value: %x", kv, op, index, args.Key, args.Value)
	kv.mu.Unlock()

	for {
		select {
		// may not receive from $ch, just block here.
		// TODO: add timeout when waiting $ch result
		case msg, ok := <-ch:
			if !ok {
				KVServerInfo("channel has been deleted in PutAppend", kv)
				*reply = PutAppendReply{WrongLeader: false, Err: ErrStaleIndex}
				return
			}

			notifiedCommand := msg.Command.(RaftKVCommand)

			KVServerInfo("Got reply -- Op: %s, ClerkId: %d. ReqSeq: %d, Key: %s, Value: %s, CommitIndex: %d", kv,
				notifiedCommand.Op, notifiedCommand.ClerkId, notifiedCommand.RequestSeq, notifiedCommand.Key, notifiedCommand.Value, msg.CommandIndex)

			// make sure $command (receive from $ch) is the same (reqSeq, clerkId) one
			if notifiedCommand.RequestSeq != command.RequestSeq || notifiedCommand.ClerkId != command.ClerkId {
				panic("should not happen! The command received should have same (reqSeq, clerkId) pair")
				//*reply = PutAppendReply{WrongLeader: true, Err: ErrUnknown}
				//return
			}

			*reply = PutAppendReply{WrongLeader: false, Err: OK}

			//kv.mu.Lock()
			//delete(kv.notifyCh, index)
			//kv.mu.Unlock()

			KVServerInfo("PutAppend RPC Return!", kv)
			return

		// KV Server and Raft in same process (goroutine) and crash at same time
		case <-time.After(CheckIsLeaderTimeout):
			kv.mu.Lock()
			if _, isLeader := kv.rf.GetState(); !isLeader {
				*reply = PutAppendReply{WrongLeader: true, Err: ErrWrongLeader}
				//delete(kv.notifyCh, index)
				kv.mu.Unlock()
				return
			} else {
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) putAppendCheckIsLeader(reply *PutAppendReply, index int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		*reply = PutAppendReply{WrongLeader: true, Err: ErrWrongLeader}
		delete(kv.notifyCh, index)
		return false
	}
	return true
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.

	kv.mu.Lock()
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.data = make(map[string]string)
	kv.notifyCh = make(map[int]chan raft.ApplyMsg)
	kv.latestRequests = make(map[int64]int64)
	kv.latestAppliedLogIndex = 0

	go kv.handleApplyMsg()
	return kv
}
