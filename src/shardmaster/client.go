package shardmaster

//
// Shardmaster clerk.
//

import (
	"labrpc"
	"log"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	clerkId ClerkIdType
	reqSeq RequestSequenceType
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}



func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = ClerkIdType(nrand())
	ck.reqSeq = 0
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	args.ClerkId = ck.clerkId
	args.ReqSeq = RequestSequenceType(atomic.AddInt64((*int64)(&ck.reqSeq), 1))

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	log.Printf("Join, %v", servers)
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	args.ClerkId = ck.clerkId
	args.ReqSeq = RequestSequenceType(atomic.AddInt64((*int64)(&ck.reqSeq), 1))

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	args.ClerkId = ck.clerkId
	args.ReqSeq = RequestSequenceType(atomic.AddInt64((*int64)(&ck.reqSeq), 1))

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	args.ClerkId = ck.clerkId
	args.ReqSeq = RequestSequenceType(atomic.AddInt64((*int64)(&ck.reqSeq), 1))

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
