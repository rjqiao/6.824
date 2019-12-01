package raftkv

import "time"

const (
	ClerkRequestTimeout  time.Duration = time.Microsecond * 400
	CheckIsLeaderTimeout time.Duration = time.Microsecond * 10
)

type CommandType int

type RaftKVCommand struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    Operation
	Key   string
	Value string

	ClerkId    int64
	RequestSeq int64
}

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrStaleIndex = "ErrStaleIndex"
	ErrUnknown    = "ErrUnknown"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	Op    		string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId     int64
	RequestSeq 	int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key 		string
	// You'll have to add definitions here.
	ClerkId     int64
	RequestSeq 	int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
