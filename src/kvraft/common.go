package kvraft

import (
	"log"
	"time"
)

const (
	ClerkRequestTimeout         time.Duration = time.Millisecond * 400
	CheckIsLeaderTimeout        time.Duration = time.Millisecond * 10
	KvServerWaitNotifyChTimeout time.Duration = time.Millisecond * 400
	ClerkRequestSleep           time.Duration = time.Millisecond * 20
)

type CommandType int

type RaftKVCommand struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string

	ClerkId    int64
	RequestSeq int64
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrStaleIndex  = "ErrStaleIndex"
	ErrUnknown     = "ErrUnknown"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId    int64
	RequestSeq int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId    int64
	RequestSeq int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

const level = 1

func KVServerInfo(format string, kv *KVServer, a ...interface{}) {
	if level <= 1 {
		args := append([]interface{}{kv.me}, a...)
		log.Printf("[INFO] RaftKV: [Id: %d] "+format, args...)
	}
}

func KVServerDebug(format string, kv *KVServer, a ...interface{}) {
	if level <= 0 {
		args := append([]interface{}{kv.me}, a...)
		log.Printf("[DEBUG] RaftKV: [Id: %d] "+format, args...)
	}
}

func KVServerTrace(format string, kv *KVServer, a ...interface{}) {
	if level <= -1 {
		args := append([]interface{}{kv.me}, a...)
		log.Printf("[TRACE] RaftKV: [Id: %d] "+format, args...)
	}
}
