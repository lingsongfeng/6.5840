package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm    KvStateMachine
	chMap map[int]chan string
}

type Command struct {
	CommandType string
	Arg0        string
	Arg1        string
	ClerkId     int
	SeqNo       int
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		DPrintf("[%v] applyCh: %#v\n", kv.me, m)
		if m.CommandValid {
			cmd := m.Command.(Command)
			uuid := m.CommandIndex
			switch cmd.CommandType {
			case "Get":
				rv := kv.sm.Get(cmd.Arg0, cmd.ClerkId, cmd.SeqNo)
				kv.sendToCh(uuid, rv)
			case "Put":
				kv.sm.Put(cmd.Arg0, cmd.Arg1, cmd.ClerkId, cmd.SeqNo)
				kv.sendToCh(uuid, "")
			case "Append":
				kv.sm.Append(cmd.Arg0, cmd.Arg1, cmd.ClerkId, cmd.SeqNo)
				kv.sendToCh(uuid, "")
			default:
				log.Fatal("unknown type")
			}
		}
	}
}

func (kv *KVServer) sendToCh(uuid int, payload string) {
	//DPrintf("send locking %v\n", kv.me)
	kv.mu.Lock()
	//DPrintf("send locked %v\n", kv.me)
	if ch, ok := kv.chMap[uuid]; ok {
		// FIXME: possible blocked here
		ch <- payload
	}
	kv.mu.Unlock()
	//DPrintf("send unlocked %v\n", kv.me)
}

func (kv *KVServer) makeAndSetCh(uuid int) <-chan string {
	ch := make(chan string, 1)

	//DPrintf("make locking %v\n", kv.me)
	kv.mu.Lock()
	//DPrintf("make locked %v\n", kv.me)
	kv.chMap[uuid] = ch
	kv.mu.Unlock()
	//DPrintf("make unlocked %v\n", kv.me)
	return ch
}

func (kv *KVServer) rmCh(uuid int) {
	//DPrintf("rm locking %v\n", kv.me)
	kv.mu.Lock()
	//DPrintf("rm locked %v\n", kv.me)
	delete(kv.chMap, uuid)
	//DPrintf("rm unlocked %v\n", kv.me)
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	uuid, _, isLeader := kv.rf.Start(Command{CommandType: "Get", Arg0: args.Key, ClerkId: args.ClerkId, SeqNo: args.SeqNo})

	done := kv.makeAndSetCh(uuid)

	if isLeader {
		// wait for commitment
		// possible timeout because of older leader
		timeout := time.After(1 * time.Second)
		select {
		case rv := <-done:
			reply.Err = OK
			reply.Value = rv
			kv.rmCh(uuid)
		case <-timeout:
			DPrintf("Get timeout\n")
			reply.Err = ErrWrongLeader
			kv.rmCh(uuid)
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("[%v] get args:%#v reply:%#v\n", kv.me, args, reply)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	uuid, _, isLeader := kv.rf.Start(Command{CommandType: "Put", Arg0: args.Key, Arg1: args.Value, ClerkId: args.ClerkId, SeqNo: args.SeqNo})

	done := kv.makeAndSetCh(uuid)

	if isLeader {
		// wait for commitment
		// possible timeout because of older leader
		timeout := time.After(1 * time.Second)
		select {
		case <-done:
			reply.Err = OK
			kv.rmCh(uuid)
		case <-timeout:
			DPrintf("Put timeout\n")
			reply.Err = ErrWrongLeader
			kv.rmCh(uuid)
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("[%v] put args:%#v reply:%#v\n", kv.me, args, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	uuid, _, isLeader := kv.rf.Start(Command{CommandType: "Append", Arg0: args.Key, Arg1: args.Value, ClerkId: args.ClerkId, SeqNo: args.SeqNo})

	done := kv.makeAndSetCh(uuid)

	if isLeader {
		// wait for commitment
		// possible timeout because of older leader
		timeout := time.After(1 * time.Second)
		select {
		case <-done:
			reply.Err = OK
			kv.rmCh(uuid)
		case <-timeout:
			DPrintf("Append timeout\n")
			reply.Err = ErrWrongLeader
			kv.rmCh(uuid)
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	DPrintf("[%v] append args:%#v reply:%#v\n", kv.me, args, reply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.chMap = make(map[int]chan string)
	kv.sm = NewKvStateMachine()

	go func() {
		kv.applier()
	}()

	return kv
}
