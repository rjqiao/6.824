package raft

import (
	"encoding/gob"
	"log"
	"time"
)

// Debugging
const Debug = 1

const (
	Follower = iota
	Candidate
	Leader
)

const (
	RaftRPCTimeout   = 50 * time.Millisecond
	HeartbeatTimeout = 120 * time.Millisecond
)

type LeaderBroadcastCommand struct{};

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	gob.Register(LeaderBroadcastCommand{})
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
		log.Printf("[DEBUG] Raft: [Id: %d | Term: %d | %v] "+format, args...)
	}
	return
}

func SendRPCRequest(requestName string, rpcTimeout time.Duration, requestBlock func() bool) bool {
	ch := make(chan bool, 1)
	exit := make(chan bool, 1)

	go func() {
		select {
		case ch <- requestBlock():
		case <-exit:
			return
		}
		// some code if requestBlock() finishes in timeout

		return
	}()

	select {
	case ok := <-ch:
		return ok
	case <-time.After(rpcTimeout):
		exit <- true
		return false
	}
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
