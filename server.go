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

func (kv *KVServer) AddRaftOp(args *OpArgs, reply *OpReply) {
	id := kv.getId()
	command := KeyValue{
		Id:    id,
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	var topic Topic
	if args.Op == GetVal {
		topic = KV_GET
	} else {
		topic = KV_PUTAPPEND
	}
	kv.logMsg(topic, fmt.Sprintf("Sent command with id %v, will wait for index %v!", id, index))
	kv.consumerCond.L.Lock()
	for kv.lastAppliedIndex != index {
		kv.logMsg(topic, fmt.Sprintf("lastAppliedIndex %v != expectedIndex %v, so will sleep", kv.lastAppliedIndex, index))
		kv.consumerCond.Wait()
	}
	kv.logMsg(topic, fmt.Sprintf("Found my expected index %v! Signaling producer...", index))
	kv.consumed = true
	kv.producerCond.Signal()
	if kv.lastAppliedId != id {
		// todo return error here.
		log.Fatalf("Not implemented!")
	}
	reply.Value = kv.lastAppliedKeyValue.Value
	kv.logMsg(topic, fmt.Sprintf("Returning value for key %v successfully!", kv.lastAppliedKeyValue.Key))
	kv.consumerCond.L.Unlock()
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
	kv.consumerCond = *sync.NewCond(&kv.mu)
	kv.producerCond = *sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.consumed = true
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
		kv.logMsg(KV_APPLYCH, fmt.Sprintf("Got new message on applyMsg %v (index %v)", applyMsg.Command, applyMsg.CommandIndex))
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
		_, isLeader := kv.rf.GetState()
		if isLeader {
			for !kv.consumed {
				kv.producerCond.Wait()
			}
			kv.consumed = false
			kv.lastAppliedId = keyValue.Id
			kv.lastAppliedIndex = applyMsg.CommandIndex
			kv.lastAppliedKeyValue.Key = key
			kv.lastAppliedKeyValue.Value = value
			kv.logMsg(KV_APPLYCH, fmt.Sprintf("Sending consumer broadcast!"))
			kv.consumerCond.Broadcast()
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
