package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const level = 0

const (
	Follower = iota
	Candidate
	Leader
)

const (
	RaftRPCTimeout        = 200 * time.Millisecond
	HeartbeatTimeout      = 50 * time.Millisecond
	electionBaseTimeout   = 400 * time.Millisecond
	electionRandomTimeout = 400 * time.Millisecond
	applyTimeout          = 100 * time.Millisecond
)

type LeaderBroadcastCommand struct{}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func RaftError(format string, rf *Raft, a ...interface{}) {
	if level <= 3 {
		args := append([]interface{}{rf.me, rf.currentTerm, rf.status}, a...)
		log.Printf("[ERROR] Raft: [Id: %d | Term: %d | %v] "+format, args...)
	}
}

func RaftWarning(format string, rf *Raft, a ...interface{}) {
	if level <= 2 {
		args := append([]interface{}{rf.me, rf.currentTerm, rf.status}, a...)
		log.Printf("[WARNING] Raft: [Id: %d | Term: %d | %v] "+format, args...)
	}
}

func RaftInfo(format string, rf *Raft, a ...interface{}) {
	if level <= 1 {
		args := append([]interface{}{rf.me, rf.currentTerm, rf.status}, a...)
		log.Printf("[INFO] Raft: [Id: %d | Term: %d | %v] "+format, args...)
	}
}

func RaftDebug(format string, rf *Raft, a ...interface{}) {
	if level <= 0 {
		args := append([]interface{}{rf.me, rf.currentTerm, rf.status}, a...)
		log.Printf("[DEBUG] Raft: [Id: %d | Term: %d | %v] "+format, args...)
	}
}

func RaftTrace(format string, rf *Raft, a ...interface{}) {
	if level <= -1 {
		args := append([]interface{}{rf.me, rf.currentTerm, rf.status}, a...)
		log.Printf("[TRACE] Raft: [Created at: %v | Id: %d | Term: %d | %v] "+format, args...)
	}
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
		ch := make(chan bool)
		go func() {
			ch <- requestBlock()
		}()

		select {
		case <-time.After(rpcTimeout):
			continue
		case res := <-ch:
			return res
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
