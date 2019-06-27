package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = 0

const RPCTimeout = 50 * time.Millisecond
const CommitApplyIdleCheckInterval = 25 * time.Millisecond

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func RaftInfo(format string, rf *Raft, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		args := append([]interface{}{rf.me, rf.currentTerm, rf.status}, a...)
		log.Printf("[INFO] Raft: [Id: %d | Term: %d | %v] "+format, args...)
	}
	return
}

func RaftDebug(format string, rf *Raft, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		args := append([]interface{}{rf.me, rf.currentTerm, rf.status}, a...)
		log.Printf("[DEBUG] Raft: [Id: %d | Term: %d | %v] "+format, args...)
	}
	return
}

func sendRPCRequest(requestName string, requestBlock func() bool) bool {
	ch := make(chan bool, 1)
	exit := make(chan bool, 1)

	go func() {
		ch <- requestBlock()

		select {
		case <-exit:
			return
		default:

		}
		// within timeout

	}()

	select {
	case ok := <-ch:
		return ok
	case <-time.After(RPCTimeout):
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
