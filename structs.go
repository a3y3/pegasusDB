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
	LEADER_WAIT       = raft.HB_WAIT_MIN // wait for these many ms before requerying for a new leader.
	PERIODIC_GET_WAIT = 500
	FAKE_CLIENT_ID    = -111
	FAKE_REQUEST_ID   = -112
)

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrNotFinishedYet = "ErrNotFinishedYet"
	ErrLogOverwritten = "ErrLogOverwritten"
)

type Clerk struct {
	servers        []*labrpc.ClientEnd
	pegasusServers []*KVServer
	currentLeader  int
	client_id      int64
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
	consumerCond        sync.Cond
	producerCond        sync.Cond
	consumed            bool
	lastAppliedIndex    int
	lastAppliedId       int64
	lastAppliedKeyValue PegasusCommand

	requests map[int64]*Request // map from client_id to request_id
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
	CK_GETPUTAPPEND  Topic = "CK_GETPUTAPPEND"
	CK_PER_GET       Topic = "CK_PER_GET"

	KV_SETUP     Topic = "KV_SETUP"
	KV_APPLYCH   Topic = "KV_APPLYCH"
	KV_GET       Topic = "KV_GET"
	KV_PUTAPPEND Topic = "KV_PUTAPPEND"
)

type Err string

type OpArgs struct {
	Key       string
	Value     string
	Op        Op
	RequestId int64
	ClientId  int64
}

type OpReply struct {
	Err   Err
	Value string // used if Op is get
}

type FindLeaderArgs struct{}
type FindLeaderReply struct {
	IsLeader bool
}

type PegasusCommand struct {
	RequestId int64
	Op        Op
	Key       string
	Value     string
}

type Request struct {
	Id     int64
	Result Result
	Index  int
}

type Result struct {
	isFinished bool
	Value      string
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
func (ck *Clerk) logMsg(topic Topic, msg string) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Milliseconds()
		log.Printf("%v %v %v %v\n", time, topic, ck.client_id, msg)
	}
}

// Logs a message with a specific topic.
func (kv *KVServer) logMsg(topic Topic, msg string) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Milliseconds()
		log.Printf("%v %v K%v %v\n", time, topic, kv.me, msg)
	}
}
