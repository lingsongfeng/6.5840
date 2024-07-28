package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int
	SeqNo   int
}

func (paa *PutAppendArgs) GetClerkId() int {
	return paa.ClerkId
}

func (paa *PutAppendArgs) GetSeqNo() int {
	return paa.SeqNo
}

type PutAppendReply struct {
	Err Err
}

func (par *PutAppendReply) SetErr(e Err) {
	par.Err = e
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int
	SeqNo   int
}

func (ga *GetArgs) GetClerkId() int {
	return ga.ClerkId
}

func (ga *GetArgs) GetSeqNo() int {
	return ga.SeqNo
}

type GetReply struct {
	Err   Err
	Value string
}

func (gr *GetReply) SetErr(e Err) {
	gr.Err = e
}

type ArgsCommon interface {
	GetClerkId() int
	GetSeqNo() int
}

type ReplyCommon interface {
	SetErr(e Err)
}
