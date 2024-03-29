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
	ck.client_id = nrand()
	ck.logMsg(CK_SETUP, fmt.Sprintf("Clerk initialized with id %v", ck.client_id))
	return ck
}

//
// shared by Get, Put and Append.
//
func (ck *Clerk) GetPutAppend(opArgs OpArgs) string {
	for true {
		opReply := OpReply{}
		ck.logMsg(CK_GETPUTAPPEND, fmt.Sprintf("Sending %v req for key %v and val %v to currentLeader", opArgs.Op, opArgs.Key, opArgs.Value))
		ok := ck.servers[ck.currentLeader].Call("KVServer.AddRaftOp", &opArgs, &opReply)
		if ok {
			if opReply.Err == ErrWrongLeader {
				ck.logMsg(CK_GETPUTAPPEND, fmt.Sprintf("Contacted wrong leader (%v), updating leader list...", ck.currentLeader))
				ck.updateCurrentLeader()
			} else if opReply.Err == "" { // no errors
				value := opReply.Value
				ck.logMsg(CK_GETPUTAPPEND, fmt.Sprintf("Returning value %v for key %v!", value, opArgs.Key))
				return value
			} else {
				// in all other errors, resend the request.
				ck.logMsg(CK_GETPUTAPPEND, fmt.Sprintf("Got err %v, re-sending req!", opReply.Err))
			}
		} else {
			ck.logMsg(CK_GETPUTAPPEND, "PutAppend RPC failed!")
			ck.updateCurrentLeader()
		}
	}
	return ""
}

func (ck *Clerk) Get(key string) string {
	opArgs := OpArgs{
		Key:       key,
		Value:     "",
		Op:        GetVal,
		RequestId: nrand(),
		ClientId:  ck.client_id,
	}
	return ck.GetPutAppend(opArgs)
}

func (ck *Clerk) Put(key string, value string) {
	opArgs := OpArgs{
		Key:       key,
		Value:     value,
		Op:        PutVal,
		RequestId: nrand(),
		ClientId:  ck.client_id,
	}
	ck.GetPutAppend(opArgs)
}
func (ck *Clerk) Append(key string, value string) {
	opArgs := OpArgs{
		Key:       key,
		Value:     value,
		Op:        AppendVal,
		RequestId: nrand(),
		ClientId:  ck.client_id,
	}
	ck.GetPutAppend(opArgs)
}

func (ck *Clerk) updateCurrentLeader() {
	leaderFound := false
	var mutex sync.Mutex
	newLeader := make(chan int)
	for i, server := range ck.servers {
		go func(i int, server *labrpc.ClientEnd) {
			for true {
				findLeaderArgs := FindLeaderArgs{}
				findLeaderReply := FindLeaderReply{}
				ok := server.Call("KVServer.IsLeader", &findLeaderArgs, &findLeaderReply)
				if ok {
					if findLeaderReply.IsLeader {
						mutex.Lock()
						ck.logMsg(CK_UPDATE_LEADER, fmt.Sprintf("Found new leader K%v", i))
						leaderFound = true
						newLeader <- i
						mutex.Unlock()
						return
					} else {
						ck.logMsg(CK_UPDATE_LEADER, fmt.Sprintf("K%v is not the leader", i))
					}
				} else {
					ck.logMsg(CK_UPDATE_LEADER, fmt.Sprintf("Failed to contact server K%v", i))
				}
				mutex.Lock()
				exit := leaderFound
				mutex.Unlock()
				if exit {
					return
				}
				// no one claims to be a leader. Wait for a while for an election, then try again.
				ck.logMsg(CK_UPDATE_LEADER, fmt.Sprintf("Going to sleep since %v is not the leader", i))
				time.Sleep(time.Millisecond * time.Duration(LEADER_WAIT))
			}
		}(i, server)
	}

	ck.currentLeader = <-newLeader
}
