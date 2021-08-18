package shardmaster

//import (
//	"raft"
//	"sync"
//)
//
//type RaftServerItf interface {
//	call()
//	WrongLeaderBuilder()
//}
//
//type RaftServer struct {
//	RaftServerItf
//
//	mu      sync.Mutex
//	me      int
//	rf      *raft.Raft
//	applyCh chan raft.ApplyMsg
//
//	lastRequests map[ClerkIdType]RequestSequenceType
//	nofityChs    map[int]chan interface{}
//
//	storage interface{}
//
//	killCh chan bool
//}
//
//func (rs *RaftServer) Kill() {
//	rs.rf.Kill()
//
//	close(rs.killCh)
//}
//
//func (rs *RaftServer) handleApplyMsgDaemon() {
//
//}
//
//type RpcReceiver struct {
//	parent *RaftServer
//}
//
//func (r *RpcReceiver) call() {
//	select {
//	case <-r.parent.killCh:
//		r.WrongLeaderBuilder()
//		return
//	}
//
//	r.parent.mu.Lock()
//	command0 := r.CommandBuilder()
//
//	index, _, isLeader := r.parent.rf.Start(command0)
//
//	if !isLeader {
//		r.WrongLeaderBuilder()
//		r.parent.
//	}
//}
//
//func (r *RpcReceiver) WrongLeaderBuilder() {
//	panic("")
//}
//
//func (r *RpcReceiver) CommandBuilder() interface{} {
//	panic("")
//}
