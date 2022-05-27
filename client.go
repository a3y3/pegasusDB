package pegasus

import (
	"crypto/rand"
	"fmt"
	"math/big"
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
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	getArgs := GetArgs{
		Key: key,
	}
	getReply := GetReply{}
	ok := ck.servers[ck.currentLeader].Call("KVServer.Get", &getArgs, &getReply)
	if ok {
		if getReply.Err == ErrWrongLeader {
			logMsg(CK_GET, fmt.Sprintf("Contacted wrong leader (%v), updating leader list...", ck.currentLeader))
			ck.updateCurrentLeader()
		}
		value := getReply.Value
		logMsg(CK_GET, fmt.Sprintf("Found value %v for key %v", value, key))
		return value
	} else {
		logMsg(CK_GET, "Get RPC failed!")
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op Op) {
	putAppendArgs := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	ok := false
	for !ok {
		putAppendReply := PutAppendReply{}
		ok = ck.servers[ck.currentLeader].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)
		if putAppendReply.Err == ErrWrongLeader {
			logMsg(CK_PUTAPPEND, fmt.Sprintf("Contacted wrong leader (%v), updating leader list...", ck.currentLeader))
			ck.updateCurrentLeader()
			ok = false
		} else {
			logMsg(CK_PUTAPPEND, fmt.Sprintf("PutAppend successful for key=%v", key))
			break
		}
		logMsg(CK_PUTAPPEND, "PutAppend RPC failed!")
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutVal)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendVal)
}

func (ck *Clerk) updateCurrentLeader() {
	for true {
		//todo parallelize this loop
		for i, server := range ck.servers {
			findLeaderArgs := FindLeaderArgs{}
			findLeaderReply := FindLeaderReply{}
			ok := server.Call("KVServer.IsLeader", &findLeaderArgs, &findLeaderReply)
			if ok {
				if findLeaderReply.IsLeader {
					ck.currentLeader = i
					logMsg(CK_UPDATE_LEADER, fmt.Sprintf("Found new leader S%v", i))
					return
				} else {
					logMsg(CK_UPDATE_LEADER, fmt.Sprintf("S%v is not the leader", i))
				}
			} else {
				logMsg(CK_UPDATE_LEADER, fmt.Sprintf("Failed to contact server S%v", i))
			}
		}
		// no one claims to be a leader. Wait for a while for an election, then try again.
		logMsg(CK_UPDATE_LEADER, "Found no leader, going to sleep...")
		time.Sleep(time.Millisecond * LEADER_WAIT)
	}
}
