package pegasus

import (
	"fmt"
	"log"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	command := Command{
		Id:    kv.getId(),
		Op:    GetVal,
		Key:   args.Key,
		Value: "",
	}
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.logMsg(KV_GET, "Started agreement for get, will wait on getCh...")
	value := <-kv.getCh
	reply.Value = value
	kv.logMsg(KV_GET, "Received value on getCh, returning")
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Command{
		Id:    kv.getId(),
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
	}
	// we're the leader! Start an agreement for this key.
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// TODO call labgob.Register on structures you want
	labgob.Register(Command{})
	// Go's RPC library to marshall/unmarshall.
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.stateMachine = make(map[string]string)
	kv.getCh = make(chan string)
	kv.logMsg(KV_SETUP, fmt.Sprintf("Initialized peagasus server S%v!", me))

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	go kv.listenOnApplyCh()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}

func (kv *KVServer) listenOnApplyCh() {
	for applyMsg := range kv.applyCh {
		kv.mu.Lock()
		command := applyMsg.Command.(Command)
		key := command.Key
		value := command.Value
		operation := command.Op
		if operation == PutVal {
			kv.stateMachine[key] = value
			kv.logMsg(KV_APPLYCH, fmt.Sprintf("Updated value for %v to %v", key, kv.stateMachine[key]))
		} else if operation == AppendVal {
			prevValue := kv.stateMachine[key]
			kv.stateMachine[key] = prevValue + value
			kv.logMsg(KV_APPLYCH, fmt.Sprintf("Append value to key %v. New value is %v", key, kv.stateMachine[key]))
		} else {
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.logMsg(KV_APPLYCH, fmt.Sprintf("Op is obviously %v, so sending on getCh", operation))
				kv.getCh <- kv.stateMachine[key]
				kv.logMsg(KV_APPLYCH, "Sent on getCh!")
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) IsLeader(args *FindLeaderArgs, reply *FindLeaderReply) {
	_, reply.IsLeader = kv.rf.GetState()
}

func (kv *KVServer) getId() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.counter += 1
	return kv.counter
}
