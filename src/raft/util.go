package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = 2

const (
	Follower = iota
	Candidate
	Leader
)

const (
	RaftRPCTimeout        = 50 * time.Millisecond
	HeartbeatTimeout      = 100 * time.Millisecond
	electionBaseTimeout   = 400 * time.Millisecond
	electionRandomTimeout = 400 * time.Millisecond
	applyTimeout          = 100 * time.Millisecond
)

type LeaderBroadcastCommand struct{}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func RaftForcePrint(format string, rf *Raft, a ...interface{}) {
	args := append([]interface{}{rf.me, rf.currentTerm, rf.status}, a...)
	log.Printf("[Force] Raft: [Id: %d | Term: %d | %v] "+format, args...)
	return
}

func RaftInfo(format string, rf *Raft, a ...interface{}) {
	if Debug > 0 {
		args := append([]interface{}{rf.me, rf.currentTerm, rf.status}, a...)
		log.Printf("[INFO] Raft: [Id: %d | Term: %d | %v] "+format, args...)
	}
	return
}

func RaftDebug(format string, rf *Raft, a ...interface{}) {
	if Debug > 1 {
		args := append([]interface{}{rf.me, rf.currentTerm, rf.status}, a...)
		log.Printf("[DEBUG] Raft: [Created at: %v | Id: %d | Term: %d | %v] "+format, args...)
	}
	return
}

func CallWhenRepeatNTimes(n int, f func()) func() {
	count := 0
	return func() {
		count++
		if count%10 == 0 {
			f()
		}
	}
}

func SendRPCRequestWithRetry(requestName string, rpcTimeout time.Duration, retryTimes int, requestBlock func() bool) bool {
	for i := 0; i < retryTimes; i++ {
		if requestBlock() {
			return true
		}
	}
	return false
}

// Max returns the larger of x or y.
func Max(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

// Min returns the smaller of x or y.
func Min(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}

func MaxInt(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func AssertF(b bool, format string, a ...interface{}) {
	if !b {
		panic(fmt.Sprintf(format, a...))
	}
}

func PanicIfF(b bool, format string, a ...interface{}) {
	if b {
		panic(fmt.Sprintf(format, a...))
	}
}
