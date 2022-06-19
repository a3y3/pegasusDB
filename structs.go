package pegasus

import (
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

const (
	LEADER_WAIT = raft.HB_WAIT_MIN // wait for these many ms before requerying for a new leader.
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Clerk struct {
	servers        []*labrpc.ClientEnd
	pegasusServers []*KVServer
	currentLeader  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	counter             int
	stateMachine        map[string]string
	cond                sync.Cond
	lastAppliedIndex    int
	lastAppliedId       int
	lastAppliedKeyValue KeyValue
}

type Op string

const (
	GetVal    Op = "GetVal"
	PutVal    Op = "PutVal"
	AppendVal Op = "AppendVal"
)

type Topic string

const (
	CK_SETUP         Topic = "CK_SETUP"
	CK_UPDATE_LEADER Topic = "CK_UPDATE_LEADER"
	CK_GET           Topic = "CK_GET"
	CK_PUTAPPEND     Topic = "CK_PUTAPPEND"

	KV_SETUP     Topic = "KV_SETUP"
	KV_APPLYCH   Topic = "KV_APPLYCH"
	KV_GET       Topic = "KV_GET"
	KV_PUTAPPEND Topic = "KV_PUTAPPEND"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    Op
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type FindLeaderArgs struct{}
type FindLeaderReply struct {
	IsLeader bool
}

type KeyValue struct {
	Id    int
	Op    Op
	Key   string
	Value string
}

// Returns the level of verbosity from stdargs.
func getVerbosity() int {
	v := os.Getenv("PEGASUS_VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

// Sets format for the default logger.
func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// Logs a message with a specific topic.
func logMsg(topic Topic, msg string) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Milliseconds()
		log.Printf("%v %v %v\n", time, topic, msg)
	}
}

// Logs a message with a specific topic.
func (kv *KVServer) logMsg(topic Topic, msg string) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Milliseconds()
		log.Printf("%v %v K%v %v\n", time, topic, kv.me, msg)
	}
}
