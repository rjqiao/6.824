package shardmaster

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"raft"
	"sort"
	"unsafe"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

const NVirtualGroup = 41

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrUnknown     = "ErrUnknown"
	ErrKilled      = "ErrKilled"
)

type Err string

type ClerkIdType int64
type RequestSequenceType int64

type HashNode interface {
	GetPos() uint32
	GetTypePoint() uint32
	GetKey() uint32
}

type ShardNode struct {
	Shard int
}

func (sn ShardNode) GetPos() uint32 {
	return hash(math.MaxUint32/NShards*uint32(sn.Shard))
	//return hash(math.MaxUint32/uint32(NShards)/2*uint32(sn.Shard) + math.MaxUint32/2)
	//return hash(math.MaxUint32/2 + uint32(sn.Shard))
}

func (sn ShardNode) GetKey() uint32 {
	return math.MaxUint32/NShards*uint32(sn.Shard)
	//return math.MaxUint32/uint32(NShards)/2*uint32(sn.Shard) + math.MaxUint32/2
	//return math.MaxUint32/2 + uint32(sn.Shard)
}

func (sn ShardNode) GetTypePoint() uint32 {
	return 0
}

type GroupNode struct {
	Group      int
	IthVirtual int
}

func (gn GroupNode) GetPos() uint32 {
	//return hash(uint32(gn.Group*NVirtualGroup) + uint32(gn.IthVirtual))
	return hash(uint32(gn.Group*NVirtualGroup*107) + uint32(gn.IthVirtual*13))
}

func (gn GroupNode) GetKey() uint32 {
	//return uint32(gn.Group*NVirtualGroup) + uint32(gn.IthVirtual)
	return uint32(gn.Group*NVirtualGroup*107) + uint32(gn.IthVirtual*13)
}

func (gn GroupNode) GetTypePoint() uint32 {
	return 1
}

type ConsistentHashing struct {
	Groups map[int][]string

	moves []MoveArgs
}

func (ch *ConsistentHashing) addGroup(servers map[int][]string) {
	for k := range servers {
		_, ok := ch.Groups[k]
		raft.AssertF(!ok, "")
		ch.Groups[k] = servers[k]
	}
}

func (ch *ConsistentHashing) moveShard(move MoveArgs) {
	raft.AssertF(move.Shard < NShards, "")
	_, ok := ch.Groups[move.GID]
	raft.AssertF(ok, "")

	for i := 0; i < len(ch.moves); i++ {
		if move.Shard == ch.moves[i].Shard {
			ch.moves[i] = move
			return
		}
	}
	ch.moves = append(ch.moves, move)
}

func (ch *ConsistentHashing) leaveGroup(GIDs []int) {
	for _, g := range GIDs {
		_, ok := ch.Groups[g]
		raft.AssertF(ok, "")
		delete(ch.Groups, g)
	}

	keys := make(map[int]bool)
	for _, g := range GIDs {
		keys[g] = true
	}

	moves2 := make([]MoveArgs, 0)
	for i := 0; i < len(ch.moves); i++ {
		if _, ok := keys[ch.moves[i].GID]; !ok {
			moves2 = append(moves2, ch.moves[i])
		}
	}
	ch.moves = moves2
}

func (ch *ConsistentHashing) generate() *Config {
	log.Printf("Generate new config\n")
	log.Printf("groups: %v", ch.Groups)
	log.Printf("moves: %v", ch.moves)

	if len(ch.Groups) == 0 {
		raft.AssertF(len(ch.moves) == 0, "")
		return &Config{
			Num:    0,
			Shards: [NShards]int{},
			Groups: make(map[int][]string),
		}
	}

	ring := make([]HashNode, 0)
	for i := 0; i < NShards; i++ {
		ring = append(ring, ShardNode{Shard: i})
	}

	for g := range ch.Groups {
		for i := 0; i < NVirtualGroup; i++ {
			ring = append(ring, GroupNode{
				Group:      g,
				IthVirtual: i,
			})
		}
	}

	sort.Slice(ring, func(i, j int) bool {
		if ring[i].GetPos() != ring[j].GetPos() {
			return ring[i].GetPos() < ring[j].GetPos()
		}
		raft.AssertF(ring[i].GetTypePoint() != ring[j].GetTypePoint(), "")
		return ring[i].GetTypePoint() < ring[j].GetTypePoint()
	})

	conf := &Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	for k := range ch.Groups {
		conf.Groups[k] = ch.Groups[k]
	}
	raft.AssertF(len(ch.Groups) > 0, "")

	firstGroup := -1
	currGroup := -1
	for i := 0; i < len(ring); i++ {
		switch node := ring[i].(type) {
		case ShardNode:
			if currGroup != -1 {
				conf.Shards[node.Shard] = ring[currGroup].(GroupNode).Group
			}
		case GroupNode:
			if firstGroup == -1 {
				firstGroup = i
			}
			currGroup = i
		default:
			panic("")
		}
	}
	raft.AssertF(firstGroup >= 0, "")

	for i := 0; i < firstGroup; i++ {
		conf.Shards[ring[i].(ShardNode).Shard] = ring[currGroup].(GroupNode).Group
	}

	for _, m := range ch.moves {
		_, ok := conf.Groups[m.GID]
		raft.AssertF(ok, "")
		conf.Shards[m.Shard] = m.GID
	}

	log.Printf("conf: %v", conf)

	ringView := make([]string, 0)
	for _, node := range ring {
		switch node2 := node.(type) {
		case ShardNode:
			ringView = append(ringView, fmt.Sprintf("ShardNode - Shard %v - GetPos %v - GetKey %v\n", node2.Shard, node2.GetPos(), node2.GetKey()))
		case GroupNode:
			ringView = append(ringView, fmt.Sprintf("GroupNode - Group %v - VirtualGroup %v - GetPos %v - GetKey %v\n", node2.Group, node2.IthVirtual, node2.GetPos(), node2.GetKey()))
		default:
			panic("")
		}
	}
	log.Printf("ringView: %v", ringView)

	return conf
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClerkId ClerkIdType
	ReqSeq  RequestSequenceType
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs    []int
	ClerkId ClerkIdType
	ReqSeq  RequestSequenceType
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ClerkId ClerkIdType
	ReqSeq  RequestSequenceType
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

	ClerkId ClerkIdType
	ReqSeq  RequestSequenceType
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func hashFnv(x uint32) uint32 {
	h := fnv.New32a()
	//bs := make([]byte, 4)
	//binary.LittleEndian.PutUint32(bs, x)
	bs := []byte(fmt.Sprint(x))
	h.Write(bs)
	return h.Sum32()
}

func hash(x uint32) uint32 {
	a := (*[4]byte)(unsafe.Pointer(&x))[:]
	bs := md5.Sum(a)
	return binary.LittleEndian.Uint32(bs[:4])
}
