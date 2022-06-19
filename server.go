package pegasus

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	id := kv.getId()
	command := KeyValue{
		Id:    id,
		Op:    GetVal,
		Key:   args.Key,
		Value: "",
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	value := ""
	kv.cond.L.Lock()
	for true {
		kv.cond.Wait()
		if kv.lastAppliedIndex == index {
			if kv.lastAppliedId == id {
				value = kv.lastAppliedKeyValue.Value
				break
			} else {
				// different id on this index? Something must have gone wrong, so ask the client to retry.
				// todo return error here.
				log.Fatalf("Not implemented!")
			}
		} else {
			kv.logMsg(KV_GET, fmt.Sprintf("Expected index %v, got %v, so waiting again", index, kv.lastAppliedIndex))
		}
	}
	kv.cond.L.Unlock()
	reply.Value = value
	kv.logMsg(KV_GET, "Returning successfully!")
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	id := kv.getId()
	command := KeyValue{
		Id:    id,
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
	}
	// we're the leader! Start an agreement for this key.
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.cond.L.Lock()
	for true {
		kv.logMsg(KV_PUTAPPEND, "Going into wait...")
		kv.cond.Wait()
		if kv.lastAppliedIndex == index {
			if kv.lastAppliedId == id {
				break
			} else {
				// todo return error here.
				log.Fatalf("Not implemented!")
			}
		} else {
			kv.logMsg(KV_GET, fmt.Sprintf("Expected index %v, got %v, so waiting again", index, kv.lastAppliedIndex))
		}
	}
	kv.cond.L.Unlock()
	kv.logMsg(KV_PUTAPPEND, "Returning successfully!")
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
	labgob.Register(KeyValue{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.stateMachine = make(map[string]string)
	kv.cond = *sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.logMsg(KV_SETUP, fmt.Sprintf("Initialized peagasus server S%v!", me))

	go kv.listenOnApplyCh()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	return kv
}

func (kv *KVServer) listenOnApplyCh() {
	for applyMsg := range kv.applyCh {
		keyValue := applyMsg.Command.(KeyValue)
		key := keyValue.Key
		value := keyValue.Value
		operation := keyValue.Op
		kv.logMsg(KV_APPLYCH, fmt.Sprintf("Got new message on applyMsg (op %v, key %v, value %v)", operation, key, value))
		kv.mu.Lock()
		if operation == PutVal {
			kv.stateMachine[key] = value
			kv.logMsg(KV_APPLYCH, fmt.Sprintf("Updated value for %v to %v", key, kv.stateMachine[key]))
		} else if operation == AppendVal {
			prevValue := kv.stateMachine[key]
			kv.stateMachine[key] = prevValue + value
			kv.logMsg(KV_APPLYCH, fmt.Sprintf("Appended value to key %v. New value is %v", key, kv.stateMachine[key]))
		} else {
			value = kv.stateMachine[key]
		}

		kv.logMsg(KV_APPLYCH, fmt.Sprintf("Sending cond broadcast!"))
		kv.lastAppliedId = keyValue.Id
		kv.lastAppliedIndex = applyMsg.CommandIndex
		kv.lastAppliedKeyValue.Value = value
		kv.cond.Broadcast()

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
