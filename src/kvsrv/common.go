package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientNo int
	Ack      int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientNo int
	Ack      int
}

type GetReply struct {
	Value string
}

type HeartbeatArgs struct {
	ClientNo int
	Ack      int
}

type HeartbeatReply struct {
}
