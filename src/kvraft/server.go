package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Operation int

const (
	Put Operation = iota
	Append
	Get
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// View of log, will eventually consistent to raft logs
	data           map[string]string          // Key -> Value
	notifyCh       map[int]chan raft.ApplyMsg // logIndex -> channel
	latestRequests map[int64]int64            // ClerkId -> ReqSeq
}

func (kv *KVServer) handleApplyMsg() {
	for {
		select {
		case msg := <-kv.applyCh:
			// Is it here to change view of state machine? Yes
			command := msg.Command.(RaftKVCommand)

			// dedup
			// because each clerk guarantees issue next request
			// after the current request succeeds
			if kv.latestRequests[command.ClerkId] != command.RequestSeq {
				switch command.Op {
				case Put:
					kv.data[command.Key] = command.Value
				case Append:
					kv.data[command.Key] += command.Value
				case Get:
				default:
				}
				kv.latestRequests[command.ClerkId] = command.RequestSeq
			}

			// if !ok, then stale or short cut request?
			// only index with request has notifyCh[index]
			if ch, ok := kv.notifyCh[msg.CommandIndex]; ok {
				ch <- msg
				close(ch)
			}
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	command := RaftKVCommand{
		Op:    Get,
		Key:   args.Key,
		Value: "",

		ClerkId:    args.ClerkId,
		RequestSeq: args.RequestSeq,
	}

	index, term, isLeader := kv.rf.Start(command)

	if !isLeader {
		*reply = GetReply{WrongLeader: true, Err: OK, Value: ""}
		return
	}

	// delete and close old channel, interrupt (old lastIndex) RPC and then return with error (maybe DeprecateReq)
	if ch, ok := kv.notifyCh[index]; ok {
		delete(kv.notifyCh, index)
		close(ch) // should only be deleted in producer side?
	}

	recvCh := make(chan raft.ApplyMsg)
	kv.notifyCh[index] = recvCh

	for {
		select {
		case msg, ok := <-recvCh:
			//term, isLeader := kv.rf.GetState()
			//if !isLeader {
			//	*reply = GetReply{WrongLeader:true, Err: ErrUnknown, Value:""}
			//	return
			//}

			if !ok {
				// stale index?
				*reply = GetReply{WrongLeader: true, Err: ErrStaleIndex}
			}

			notifiedCommand := msg.Command.(RaftKVCommand)

			// might happen !!!
			if notifiedCommand != command {
				panic("should not happen! notifiedCommand != command")

				//*reply = GetReply{WrongLeader: true, Err: ErrUnknown}
				//return
			}

			*reply = GetReply{WrongLeader: false, Err: OK, Value: kv.data[command.Key],}
			delete(kv.notifyCh, index)
			return


		case <-time.After(CheckIsLeaderTimeout):
			if _, isLeader := kv.rf.GetState(); !isLeader {
				*reply = GetReply{WrongLeader:true, Err:ErrUnknown}
				delete(kv.notifyCh, index)
				return
			}
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Put
	if args.Op == "Append" {
		op = Append
	}

	// fast reply

	// wait until get from ApplyMsg
	command := RaftKVCommand{
		Op:         op,
		ClerkId:    args.ClerkId,
		RequestSeq: args.RequestSeq,
	}

	index, term, isLeader := kv.rf.Start(command)

	if !isLeader {
		*reply = PutAppendReply{WrongLeader: false, Err: OK}
		return
	}

	kv.notifyCh[index] = make(chan raft.ApplyMsg)

	select {
	case msg := <-kv.notifyCh[index]:
		notifiedCommand := msg.Command.(RaftKVCommand)
		if notifiedCommand != command {
			*reply = PutAppendReply{WrongLeader: true, Err: ErrUnknown}
		} else {
			*reply = PutAppendReply{WrongLeader: false, Err: OK}
		}
	}

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

	return kv
}
