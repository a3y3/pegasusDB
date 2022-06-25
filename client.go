package pegasus

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	logMsg(CK_SETUP, "Clerk initializing!")
	return ck
}

//
// shared by Get, Put and Append.
//
func (ck *Clerk) GetPutAppend(key string, value string, op Op) string {
	requestId := nrand()
	opArgs := OpArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		RequestId: requestId,
	}
	for true {
		opReply := OpReply{}
		logMsg(CK_GETPUTAPPEND, fmt.Sprintf("Sending %v req for key %v and val %v to currentLeader", op, key, value))
		ok := ck.servers[ck.currentLeader].Call("KVServer.AddRaftOp", &opArgs, &opReply)
		if ok {
			if opReply.Err == ErrWrongLeader {
				logMsg(CK_GETPUTAPPEND, fmt.Sprintf("Contacted wrong leader (%v), updating leader list...", ck.currentLeader))
				ck.updateCurrentLeader()
			} else {
				value := opReply.Value
				logMsg(CK_GETPUTAPPEND, fmt.Sprintf("Returning value %v for key %v!", value, key))
				return value
			}
		} else {
			logMsg(CK_GETPUTAPPEND, "PutAppend RPC failed!")
		}
	}
	return ""
}

func (ck *Clerk) Get(key string) string {
	return ck.GetPutAppend(key, "", GetVal)
}

func (ck *Clerk) Put(key string, value string) {
	ck.GetPutAppend(key, value, PutVal)
}
func (ck *Clerk) Append(key string, value string) {
	ck.GetPutAppend(key, value, AppendVal)
}

func (ck *Clerk) updateCurrentLeader() {
	leaderFound := false
	var mutex sync.Mutex
	cond := sync.NewCond(&mutex)
	for i, server := range ck.servers {
		go func(i int, server *labrpc.ClientEnd) {
			for true {
				mutex.Lock()
				exit := leaderFound
				mutex.Unlock()
				if exit {
					return
				}
				findLeaderArgs := FindLeaderArgs{}
				findLeaderReply := FindLeaderReply{}
				ok := server.Call("KVServer.IsLeader", &findLeaderArgs, &findLeaderReply)
				if ok {
					if findLeaderReply.IsLeader {
						ck.currentLeader = i
						logMsg(CK_UPDATE_LEADER, fmt.Sprintf("Found new leader S%v", i))
						mutex.Lock()
						leaderFound = true
						cond.Signal()
						mutex.Unlock()
						return
					} else {
						logMsg(CK_UPDATE_LEADER, fmt.Sprintf("K%v is not the leader", i))
					}
				} else {
					logMsg(CK_UPDATE_LEADER, fmt.Sprintf("Failed to contact server K%v", i))
				}

				// no one claims to be a leader. Wait for a while for an election, then try again.
				logMsg(CK_UPDATE_LEADER, "Found no leader, going to sleep...")
				time.Sleep(time.Millisecond * time.Duration(LEADER_WAIT))
			}
		}(i, server)
	}

	cond.L.Lock()
	for !leaderFound {
		cond.Wait()
	}
	cond.L.Unlock()
}
