package kvsrv

import (
	"log"
	"sync"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	mp    map[string]string
	cache map[int]*SlidingWindow
}

func (kv *KVServer) checkClientCache(clientNo int) {
	if _, ok := kv.cache[clientNo]; !ok {
		kv.cache[clientNo] = NewSlidingWindow()
	}
}

func (kv *KVServer) queryFromCache(clientNo int, seqNo int) (interface{}, bool) {
	return kv.cache[clientNo].FindBySeqNo(seqNo)
}

func (kv *KVServer) SaveToCache(clientNo int, seqNo int, payload interface{}) {
	kv.cache[clientNo].Insert(payload, seqNo)
}

func (kv *KVServer) ReleaseAck(clinetNo int, ack int) {
	kv.cache[clinetNo].Relax(ack)
	if kv.cache[clinetNo].deque.Len() == 0 {
		delete(kv.cache, clinetNo)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	start := time.Now()
	kv.checkClientCache(args.ClientNo)

	if v, ok := kv.mp[args.Key]; ok {
		reply.Value = v
	} else {
		reply.Value = ""
	}
	kv.ReleaseAck(args.ClientNo, args.Ack)

	DPrintf("get delta t=%v\n", time.Since(start))
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	start := time.Now()

	kv.checkClientCache(args.ClientNo)

	if payload, ok := kv.queryFromCache(args.ClientNo, args.Ack); ok {
		reply.Value = payload.(string)
	} else {
		kv.mp[args.Key] = args.Value
		kv.SaveToCache(args.ClientNo, args.Ack, reply.Value)
		kv.ReleaseAck(args.ClientNo, args.Ack)
	}

	DPrintf("put delta t=%v\n", time.Since(start))
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	start := time.Now()

	kv.checkClientCache(args.ClientNo)

	if payload, ok := kv.queryFromCache(args.ClientNo, args.Ack); ok {
		reply.Value = payload.(string)
	} else {
		reply.Value = kv.mp[args.Key]
		kv.mp[args.Key] = reply.Value + args.Value
		kv.SaveToCache(args.ClientNo, args.Ack, reply.Value)
		kv.ReleaseAck(args.ClientNo, args.Ack)
	}
	DPrintf("append delta t=%v\n", time.Since(start))
}

func (kv *KVServer) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	start := time.Now()

	kv.checkClientCache(args.ClientNo)
	kv.ReleaseAck(args.ClientNo, args.Ack)

	DPrintf("heartbeat delta t=%v\n", time.Since(start))
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mp = make(map[string]string)
	kv.cache = make(map[int]*SlidingWindow)

	return kv
}
