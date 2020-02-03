package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"
import "sync/atomic"
import "raft"

var (
	_clerkId int64 = 0
)

func GenerateId() int64 {
	return atomic.AddInt64(&_clerkId, 1) - 1
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Id         int64
	ReqSeq     int64
	lastLeader int
}

func (ck *Clerk) GenerateReqSeq() int64 {
	return atomic.AddInt64(&ck.ReqSeq, 1) - 1
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
	// You'll have to add code here.
	ck.Id = GenerateId()
	ck.ReqSeq = nrand()
	ck.lastLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	args := &GetArgs{Key: key}
	reply := &GetReply{}

	count := 0
	i := ck.lastLeader
	for reply.Err != OK && reply.Err != ErrNoKey {
		server := ck.servers[i%len(ck.servers)]
		requestBlock := func() bool {
			*reply = GetReply{}
			return server.Call("KVServer.Get", args, reply)
		}


		//log.Printf("PutAppend, to server %d, key: %s, value: %s, op: %s, ClerkId: %v, ReqSeq: %v, times: %d",
		//	i%len(ck.servers), key, reply.Value, "GET", ck.Id, args.RequestSeq, count)

		ok := raft.SendRPCRequest("KVServer.Get", ClerkRequestTimeout, requestBlock)

		//log.Printf("PutAppend, to server %d, key: %s, value: %s, op: %s, ClerkId: %v, ReqSeq: %v, times: %d, ok?: %t, err: %s, wrongleader: %t",
		//	i%len(ck.servers), key, reply.Value, "GET", ck.Id, args.RequestSeq, count, ok, reply.Err, reply.WrongLeader)

		count++

		if !ok || reply.WrongLeader {
			i++
		} else {
			ck.lastLeader = i
		}

		time.Sleep(ClerkRequestSleep)
	}
	//log.Printf("Clerk %v -- Get, key: %v, value:%v, Err: %v", ck, key, reply.Value, reply.Err)
	if reply.Err == ErrNoKey {
		return ""
	} else {
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClerkId: ck.Id, RequestSeq: ck.GenerateReqSeq()}

	count := 0
	i := ck.lastLeader
	for {
		server := ck.servers[i%len(ck.servers)]

		reply := PutAppendReply{} // reset to default value
		requestBlock := func() bool {
			reply = PutAppendReply{}
			return server.Call("KVServer.PutAppend", &args, &reply)
		}

		//log.Printf("PutAppend, to server %d, key: %s, value: %s, op: %s, ClerkId: %v, ReqSeq: %v, times: %d",
		//	i%len(ck.servers), key, value, op, ck.Id, args.RequestSeq, count)

		ok := raft.SendRPCRequest("KVServer.PutAppend", ClerkRequestTimeout, requestBlock)

		//log.Printf("PutAppend, to server %d, key: %s, value: %s, op: %s, ClerkId: %v, ReqSeq: %v, times: %d, ok?: %t, err: %s, wrongleader: %t",
		//	i%len(ck.servers), key, value, op, ck.Id, args.RequestSeq, count, ok, reply.Err, reply.WrongLeader)

		count++

		if !ok || reply.WrongLeader {
			i++
		} else {
			ck.lastLeader = i
		}

		if ok && reply.Err == OK {
			break
		}

		time.Sleep(ClerkRequestSleep)
	}
}

func (ck *Clerk) Put(key string, value string) {
	//log.Printf("Clerk %v -- Put, key: %v, value:%v", ck, key, value)
	ck.PutAppend(key, value, "Put")
	//log.Printf("Clerk %v -- Put finished, key: %v, value:%v", ck, key, value)
}
func (ck *Clerk) Append(key string, value string) {
	//log.Printf("Clerk %v -- Append, key: %v, value:%v", ck, key, value)
	ck.PutAppend(key, value, "Append")
	//log.Printf("Clerk %v -- Append finished, key: %v, value:%v", ck, key, value)
}
