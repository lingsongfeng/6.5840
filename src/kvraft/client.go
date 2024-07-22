package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId    int
	ack        int
	mu         sync.Mutex
	currLeader int
	currTerm   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = int(nrand())
	ck.ack = 1
	ck.currLeader = 0
	ck.mu = sync.Mutex{}
	return ck
}

func (ck *Clerk) findLeaderAndCall(method string, args interface{}, reply interface{}) string {
	for {
		for !ck.servers[ck.currLeader].Call(method, args, reply) {
			ck.currLeader++
			ck.currLeader %= len(ck.servers)
		}
		rv := "default rv"
		var err Err
		switch r := reply.(type) {
		case *PutAppendReply:
			err = r.Err
		case *GetReply:
			rv = r.Value
			err = r.Err
		default:
			panic("unknown type")
		}
		if err == ErrWrongLeader {
			ck.currLeader++
			ck.currLeader %= len(ck.servers)
		} else if err == OK {
			return rv
		} else {
			panic(fmt.Sprintf("unknown err = %v", err))
		}
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{Key: key, ClerkId: ck.clerkId, SeqNo: ck.ack}
	reply := GetReply{}
	for {
		for !ck.servers[ck.currLeader].Call("KVServer.Get", &args, &reply) {
			ck.currLeader++
			ck.currLeader %= len(ck.servers)
		}
		if reply.Err == ErrWrongLeader {
			ck.currLeader++
			ck.currLeader %= len(ck.servers)
		} else if reply.Err == OK {
			break
		} else {
			panic(fmt.Sprintf("unknown err = %v", reply.Err))
		}
	}

	ck.ack++
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := PutAppendArgs{Key: key, Value: value, ClerkId: ck.clerkId, SeqNo: ck.ack}
	reply := PutAppendReply{}
	ck.findLeaderAndCall("KVServer."+op, &args, &reply)
	ck.ack++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
