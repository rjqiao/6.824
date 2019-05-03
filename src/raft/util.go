package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = 1

const RPCTimeout = 30 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
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