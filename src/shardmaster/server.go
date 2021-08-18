package shardmaster

import (
	"kvraft"
	"log"
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

type ShardMasterCommand struct {
	// Your data here.
	Op      string
	Servers map[int][]string // Join
	GIDs    []int            // Leave
	Shard   int              // move
	GID     int              // move
	Num     int              // query
	Config  Config           // query reply

	ClerkId ClerkIdType
	ReqSeq  RequestSequenceType
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastRequests map[ClerkIdType]RequestSequenceType
	// Index could be reused in some corner cases
	notifyChs map[int]chan ShardMasterCommand

	configs []Config // indexed by config num

	ch ConsistentHashing

	killCh chan bool
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	select {
	case <-sm.killCh:
		*reply = JoinReply{
			WrongLeader: false,
			Err:         ErrKilled,
		}
		return
	default:
	}

	log.Printf("receive Join, %v", args)

	// ShardMaster Locks outside Raft's lock
	sm.mu.Lock()
	command0 := ShardMasterCommand{
		Op:      "Join",
		ClerkId: args.ClerkId,
		ReqSeq:  args.ReqSeq,
		Servers: args.Servers,
	}
	index, _, isLeader := sm.rf.Start(command0)

	if !isLeader {
		*reply = JoinReply{
			WrongLeader: true,
			Err:         ErrWrongLeader,
		}
		sm.mu.Unlock()
		return
	}

	_, ok := sm.notifyChs[index]
	raft.AssertF(!ok, "")

	ch := make(chan ShardMasterCommand)
	sm.notifyChs[index] = ch
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		// remove $ch from notifyChs
		ch2, ok := sm.notifyChs[index]
		raft.AssertF(!ok || ch2 == ch, "")
		if ok && ch2 == ch {
			delete(sm.notifyChs, index)
		}
		sm.mu.Unlock()
	}()

	timer := time.After(kvraft.ClerkRequestTimeout)
	// check leader every CheckIsLeaderTimeout
	// timeout in ClerkRequestTimeout
	// wait on $ch
	for {
		select {
		case <-timer:
			*reply = JoinReply{
				WrongLeader: false,
				Err:         ErrUnknown,
			}
			return
		case <-time.After(kvraft.CheckIsLeaderTimeout):
			sm.mu.Lock()
			if _, isLeader := sm.rf.GetState(); !isLeader {
				*reply = JoinReply{
					WrongLeader: true,
					Err:         ErrWrongLeader,
				}
				sm.mu.Unlock()
				return
			}
			sm.mu.Unlock()
		default:
			command, ok := <-ch
			raft.AssertF(ok, "should not be closed")
			if !ok {
				*reply = JoinReply{
					WrongLeader: false,
					Err:         ErrUnknown,
				}
				return
			}
			raft.AssertF(command.ClerkId == command0.ClerkId &&
				command.ReqSeq == command0.ReqSeq,
				"")

			*reply = JoinReply{
				WrongLeader: false,
				Err:         OK,
			}
			return
		}
	}

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	select {
	case <-sm.killCh:
		*reply = LeaveReply{
			WrongLeader: false,
			Err:         ErrKilled,
		}
		return
	default:
	}

	sm.mu.Lock()
	command0 := ShardMasterCommand{
		Op:      "Leave",
		ClerkId: args.ClerkId,
		ReqSeq:  args.ReqSeq,
		GIDs:    args.GIDs,
	}
	index, _, isLeader := sm.rf.Start(command0)

	if !isLeader {
		*reply = LeaveReply{
			WrongLeader: true,
			Err:         ErrWrongLeader,
		}
		sm.mu.Unlock()
		return
	}

	_, ok := sm.notifyChs[index]
	raft.AssertF(!ok, "")

	ch := make(chan ShardMasterCommand)
	sm.notifyChs[index] = ch
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		// remove $ch from notifyChs
		ch2, ok := sm.notifyChs[index]
		raft.AssertF(!ok || ch2 == ch, "")
		if ok && ch2 == ch {
			delete(sm.notifyChs, index)
		}
		sm.mu.Unlock()
	}()

	timer := time.After(kvraft.ClerkRequestTimeout)
	// check leader every CheckIsLeaderTimeout
	// timeout in ClerkRequestTimeout
	// wait on $ch
	for {
		select {
		case <-timer:
			*reply = LeaveReply{
				WrongLeader: false,
				Err:         ErrUnknown,
			}
			return
		case <-time.After(kvraft.CheckIsLeaderTimeout):
			sm.mu.Lock()
			if _, isLeader := sm.rf.GetState(); !isLeader {
				*reply = LeaveReply{
					WrongLeader: true,
					Err:         ErrWrongLeader,
				}
				sm.mu.Unlock()
				return
			}
			sm.mu.Unlock()
		default:
			command, ok := <-ch
			raft.AssertF(ok, "should not be closed")
			if !ok {
				*reply = LeaveReply{
					WrongLeader: false,
					Err:         ErrUnknown,
				}
				return
			}
			raft.AssertF(command.ClerkId == command0.ClerkId &&
				command.ReqSeq == command0.ReqSeq,
				"")

			*reply = LeaveReply{
				WrongLeader: false,
				Err:         OK,
			}
			return
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	select {
	case <-sm.killCh:
		*reply = MoveReply{
			WrongLeader: false,
			Err:         ErrKilled,
		}
		return
	default:
	}

	// ShardMaster Locks outside Raft's lock
	sm.mu.Lock()
	command0 := ShardMasterCommand{
		Op:      "Move",
		ClerkId: args.ClerkId,
		ReqSeq:  args.ReqSeq,
		GID:     args.GID,
		Shard:   args.Shard,
	}
	index, _, isLeader := sm.rf.Start(command0)

	if !isLeader {
		*reply = MoveReply{
			WrongLeader: true,
			Err:         ErrWrongLeader,
		}
		sm.mu.Unlock()
		return
	}

	_, ok := sm.notifyChs[index]
	raft.AssertF(!ok, "")

	ch := make(chan ShardMasterCommand)
	sm.notifyChs[index] = ch
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		// remove $ch from notifyChs
		ch2, ok := sm.notifyChs[index]
		raft.AssertF(!ok || ch2 == ch, "")
		if ok && ch2 == ch {
			delete(sm.notifyChs, index)
		}
		sm.mu.Unlock()
	}()

	timer := time.After(kvraft.ClerkRequestTimeout)
	// check leader every CheckIsLeaderTimeout
	// timeout in ClerkRequestTimeout
	// wait on $ch
	for {
		select {
		case <-timer:
			*reply = MoveReply{
				WrongLeader: false,
				Err:         ErrUnknown,
			}
			return
		case <-time.After(kvraft.CheckIsLeaderTimeout):
			sm.mu.Lock()
			if _, isLeader := sm.rf.GetState(); !isLeader {
				*reply = MoveReply{
					WrongLeader: true,
					Err:         ErrWrongLeader,
				}
				sm.mu.Unlock()
				return
			}
			sm.mu.Unlock()
		default:
			command, ok := <-ch
			raft.AssertF(ok, "should not be closed")
			if !ok {
				*reply = MoveReply{
					WrongLeader: false,
					Err:         ErrUnknown,
				}
				return
			}
			raft.AssertF(command.ClerkId == command0.ClerkId &&
				command.ReqSeq == command0.ReqSeq,
				"")

			*reply = MoveReply{
				WrongLeader: false,
				Err:         OK,
			}
			return
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	select {
	case <-sm.killCh:
		*reply = QueryReply{
			WrongLeader: false,
			Err:         ErrKilled,
		}
		return
	default:
	}

	// ShardMaster Locks outside Raft's lock
	sm.mu.Lock()
	command0 := ShardMasterCommand{
		Op:      "Query",
		ClerkId: args.ClerkId,
		ReqSeq:  args.ReqSeq,
		Num:     args.Num,
	}
	index, _, isLeader := sm.rf.Start(command0)

	if !isLeader {
		*reply = QueryReply{
			WrongLeader: true,
			Err:         ErrWrongLeader,
		}
		sm.mu.Unlock()
		return
	}

	_, ok := sm.notifyChs[index]
	raft.AssertF(!ok, "")

	ch := make(chan ShardMasterCommand)
	sm.notifyChs[index] = ch
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		// remove $ch from notifyChs
		ch2, ok := sm.notifyChs[index]
		raft.AssertF(!ok || ch2 == ch, "")
		if ok && ch2 == ch {
			delete(sm.notifyChs, index)
		}
		sm.mu.Unlock()
	}()

	timer := time.After(kvraft.ClerkRequestTimeout)
	// check leader every CheckIsLeaderTimeout
	// timeout in ClerkRequestTimeout
	// wait on $ch
	for {
		select {
		case <-timer:
			*reply = QueryReply{
				WrongLeader: false,
				Err:         ErrUnknown,
			}
			return
		case <-time.After(kvraft.CheckIsLeaderTimeout):
			sm.mu.Lock()
			if _, isLeader := sm.rf.GetState(); !isLeader {
				*reply = QueryReply{
					WrongLeader: true,
					Err:         ErrWrongLeader,
				}
				sm.mu.Unlock()
				return
			}
			sm.mu.Unlock()
		default:
			command, ok := <-ch
			raft.AssertF(ok, "should not be closed")
			if !ok {
				*reply = QueryReply{
					WrongLeader: false,
					Err:         ErrUnknown,
				}
				return
			}
			raft.AssertF(command.ClerkId == command0.ClerkId &&
				command.ReqSeq == command0.ReqSeq,
				"")

			*reply = QueryReply{
				WrongLeader: false,
				Err:         OK,
				Config:      command.Config,
			}
			return
		}
	}

}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.killCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) handleApplyMsgDaemon() {
	for {
		select {
		case <-sm.killCh:
			return
		default:
		}

		select {
		case msg := <-sm.applyCh:
			if !msg.CommandValid {
				// snapshot
				panic("")
				continue
			}

			handleNewLog := func() {
				command := msg.Command.(ShardMasterCommand)

				sm.mu.Lock()
				defer func() {
					if ch, ok := sm.notifyChs[msg.CommandIndex]; ok {
						delete(sm.notifyChs, msg.CommandIndex)
						sm.mu.Unlock()
						go func() {
							ch <- command
							close(ch)
						}()
					} else {
						sm.mu.Unlock()
					}
				}()

				seq, ok := sm.lastRequests[command.ClerkId]
				if ok && command.ReqSeq <= seq {
					// handle duplicate seq num from same client
				} else {
					sm.lastRequests[command.ClerkId] = command.ReqSeq

					// handle new seq
					switch command.Op {
					case "Join":
						log.Printf("Apply Join, %v", command)
						sm.ch.addGroup(command.Servers)
						sm.configs = append(sm.configs, *sm.ch.generate())
						sm.configs[len(sm.configs)-1].Num = len(sm.configs) - 1
					case "Leave":
						log.Printf("Apply Leave")
						sm.ch.leaveGroup(command.GIDs)
						sm.configs = append(sm.configs, *sm.ch.generate())
						sm.configs[len(sm.configs)-1].Num = len(sm.configs) - 1
					case "Move":
						log.Printf("Apply Move")
						sm.ch.moveShard(MoveArgs{
							Shard: command.Shard,
							GID:   command.GID,
						})
						sm.configs = append(sm.configs, *sm.ch.generate())
						sm.configs[len(sm.configs)-1].Num = len(sm.configs) - 1
					case "Query":
						log.Printf("Apply Query")
						if command.Num == -1 {
							command.Config = sm.configs[len(sm.configs)-1]
						} else {
							raft.AssertF(command.Num >= 0 && command.Num < len(sm.configs), "")
							command.Config = sm.configs[command.Num]
						}
					}
				}
			}

			handleNewLog()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(ShardMasterCommand{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.killCh = make(chan bool)

	sm.lastRequests = make(map[ClerkIdType]RequestSequenceType)
	sm.notifyChs = make(map[int]chan ShardMasterCommand)

	sm.ch.moves = make([]MoveArgs, 0)
	sm.ch.Groups = make(map[int][]string)

	go sm.handleApplyMsgDaemon()

	return sm
}
