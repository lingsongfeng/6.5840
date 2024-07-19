package kvsrv

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	uuid int
	ack  int
	mu   sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.uuid = int(nrand())
	ck.ack = 0
	ck.mu = sync.Mutex{}

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	// You will have to modify this function.
	args := GetArgs{Key: key, ClientNo: ck.uuid, Ack: ck.ack}
	reply := GetReply{}
	for !ck.server.Call("KVServer.Get", &args, &reply) {
		time.Sleep(100 * time.Millisecond)
	}
	ck.SendHeartbeat(false)
	ck.ack++
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, ClientNo: ck.uuid, Ack: ck.ack}
	reply := PutAppendReply{}

	for !ck.server.Call("KVServer."+op, &args, &reply) {
	}
	ck.SendHeartbeat(false)
	ck.ack++
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) SendHeartbeat(lock bool) {
	if lock {
		ck.mu.Lock()
		defer ck.mu.Unlock()
	}

	args := HeartbeatArgs{ClientNo: ck.uuid, Ack: ck.ack}
	ck.ack++
	reply := HeartbeatReply{}
	for !ck.server.Call("KVServer.Heartbeat", &args, &reply) {
		break
		//time.Sleep(100 * time.Millisecond)
	}
}
