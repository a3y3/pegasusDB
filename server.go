package pegasus

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

func (kv *KVServer) AddRaftOp(args *OpArgs, reply *OpReply) {
	var topic Topic
	if args.Op == GetVal {
		topic = KV_GET
	} else {
		topic = KV_PUTAPPEND
	}
	requestId := args.RequestId
	clientId := args.ClientId

	kv.mu.Lock()
	existingReq, ok := kv.requests[clientId]
	kv.mu.Unlock()
	if ok && existingReq.Id == requestId {
		// duplicate request!
		result := existingReq.Result
		if !result.isFinished {
			// the old thread hasn't gotten the result yet - we can tell this to the client who can resubmit this req after a timeout.
			reply.Err = ErrNotFinishedYet
			return
		}
		kv.logMsg(topic, fmt.Sprintf("Duplicate request found for requestId %v! Returning existing result %v", requestId, result.Value))
		reply.Value = result.Value
		return
	}

	command := PegasusCommand{
		RequestId: requestId,
		Op:        args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}

	kv.consumerCond.L.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.consumerCond.L.Unlock()
		return
	}
	if requestId == FAKE_REQUEST_ID {
		// process no further, this req exists only as an HB
		kv.consumerCond.L.Unlock()
		return
	}
	kv.logMsg(topic, fmt.Sprintf("Sent command with id %v, will wait for index %v!", requestId, index))

	kv.requests[clientId] = &Request{Id: requestId, Index: index}
	for kv.lastAppliedIndex != index {
		kv.logMsg(topic, fmt.Sprintf("lastAppliedIndex %v != expectedIndex %v, so will sleep", kv.lastAppliedIndex, index))
		kv.consumerCond.Wait()
	}
	kv.logMsg(topic, fmt.Sprintf("Found my expected index %v! Signaling producer...", index))
	kv.consumed = true
	kv.producerCond.Signal()
	if kv.lastAppliedId != requestId {
		kv.logMsg(topic, fmt.Sprintf("Found a different log message id (%v) than expected (%v)", kv.lastAppliedId, requestId))
		reply.Err = ErrLogOverwritten
		delete(kv.requests, clientId)
		kv.consumerCond.L.Unlock()
		return
	}
	reply.Value = kv.lastAppliedKeyValue.Value
	currentRequest := kv.requests[clientId]
	currentRequest.Result = Result{isFinished: true, Value: reply.Value}

	kv.logMsg(topic, fmt.Sprintf("Stored value %v, and returning it for key %v successfully!", reply.Value, kv.lastAppliedKeyValue.Key))
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
	labgob.Register(PegasusCommand{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.stateMachine = make(map[string]string)
	kv.consumerCond = *sync.NewCond(&kv.mu)
	kv.producerCond = *sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.consumed = true
	kv.requests = make(map[int64]*Request)
	kv.duplicate = make(map[int64]bool)
	kv.logMsg(KV_SETUP, fmt.Sprintf("Initialized peagasus server S%v!", me))

	go kv.listenOnApplyCh()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.sendPeriodicGet()
	return kv
}

func (kv *KVServer) listenOnApplyCh() {
	for applyMsg := range kv.applyCh {
		pegasusCommand := applyMsg.Command.(PegasusCommand)
		key := pegasusCommand.Key
		value := pegasusCommand.Value
		operation := pegasusCommand.Op

		kv.logMsg(KV_APPLYCH, fmt.Sprintf("Got new message on applyMsg %v (index %v)", applyMsg.Command, applyMsg.CommandIndex))
		kv.mu.Lock()
		_, duplicate := kv.duplicate[pegasusCommand.RequestId]
		if !duplicate && operation == PutVal {
			kv.stateMachine[key] = value
			kv.logMsg(KV_APPLYCH, fmt.Sprintf("Finished operation PutVal. Updated value for %v to %v", key, kv.stateMachine[key]))
		} else if !duplicate && operation == AppendVal {
			prevValue := kv.stateMachine[key]
			kv.stateMachine[key] = prevValue + value
			kv.logMsg(KV_APPLYCH, fmt.Sprintf("Finished operation AppendVal for key %v. New value is %v", key, kv.stateMachine[key]))
		} else {
			value = kv.stateMachine[key]
		}

		kv.duplicate[pegasusCommand.RequestId] = true

		kv.lastAppliedId = pegasusCommand.RequestId
		kv.lastAppliedIndex = applyMsg.CommandIndex
		kv.lastAppliedKeyValue.Key = key
		kv.lastAppliedKeyValue.Value = value
		kv.consumed = false
		kv.logMsg(KV_APPLYCH, fmt.Sprintf("Sending consumer broadcast!"))
		kv.consumerCond.Broadcast()

		// wait for the value we just produced to be consumed, just before producing another value
		// However, before waiting, we need to check if someone is even expecting this message.
		// go through the requests map, and see if any values match this CommandIndex.
		someonesWaiting := false
		for _, v := range kv.requests {
			if v.Index == applyMsg.CommandIndex {
				someonesWaiting = true
				break
			}
		}
		if someonesWaiting {
			// wait for the current value to be consumed by them.
			for !kv.consumed {
				kv.logMsg(KV_APPLYCH, fmt.Sprintf("Going into wait... context: index=%v", applyMsg.CommandIndex))
				kv.producerCond.Wait()
				kv.logMsg(KV_APPLYCH, fmt.Sprintf("Awoken! context: index=%v", applyMsg.CommandIndex))
			}
		} else {
			kv.logMsg(KV_APPLYCH, fmt.Sprintf("Skipped waiting because no one is waiting for index %v", applyMsg.CommandIndex))
			kv.consumed = true // treat the current value to be consumed because no one is going to (and some previous leader already did)
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) sendPeriodicGet() {
	for {
		time.Sleep(time.Millisecond * PERIODIC_GET_WAIT)
		opArgs := OpArgs{
			Key:       "fake key",
			Op:        GetVal,
			RequestId: FAKE_REQUEST_ID,
			ClientId:  FAKE_CLIENT_ID,
		}
		kv.AddRaftOp(&opArgs, &OpReply{})
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
