package raftkv

import "labrpc"
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

	i := ck.lastLeader
	for reply.Err != OK && reply.Err != ErrNoKey {
		server := ck.servers[i%len(ck.servers)]
		requestBlock := func() bool { return server.Call("KVServer.Get", args, reply) }
		ok := raft.SendRPCRequest("KVServer.Get", ClerkRequestTimeout, requestBlock)

		if !ok || reply.WrongLeader {
			i++
		} else {
			ck.lastLeader = i
		}
	}

	if reply.Err == ErrNoKey {
		return ""
	}
	return reply.Value
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
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ClerkId: ck.Id, RequestSeq: ck.GenerateReqSeq()}
	reply := &PutAppendReply{}

	i := ck.lastLeader
	for reply.Err != OK {
		server := ck.servers[i%len(ck.servers)]
		requestBlock := func() bool {return server.Call("KVServer.PutAppend", args, reply)}

		ok := raft.SendRPCRequest("KVServer.PutAppend", ClerkRequestTimeout, requestBlock)

		if !ok || reply.WrongLeader {
			i++
		} else {
			ck.lastLeader = i
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
