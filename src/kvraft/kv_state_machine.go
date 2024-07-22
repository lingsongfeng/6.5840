package kvraft

import "sync"

type KvStateMachine struct {
	mu    sync.Mutex
	kvMap map[string]string
	seqNo map[int]int
}

func NewKvStateMachine() KvStateMachine {
	return KvStateMachine{
		mu:    sync.Mutex{},
		kvMap: make(map[string]string),
		seqNo: make(map[int]int),
	}
}
func (sm *KvStateMachine) Get(key string, clerkId, seqNo int) string {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	/*
		if seqNo <= sm.seqNo[clerkId] {
			return "EXPIRED GET"
		} else {
			sm.seqNo[clerkId] = seqNo
			return sm.kvMap[key]
		}
	*/
	if seqNo > sm.seqNo[clerkId] {
		sm.seqNo[clerkId] = seqNo
	}
	return sm.kvMap[key]
}

func (sm *KvStateMachine) Put(key, value string, clerkId, seqNo int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if seqNo <= sm.seqNo[clerkId] {
		return
	} else {
		sm.seqNo[clerkId] = seqNo
		sm.kvMap[key] = value
	}
}

func (sm *KvStateMachine) Append(key, arg string, clerkId, seqNo int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if seqNo <= sm.seqNo[clerkId] {
		return
	} else {
		sm.seqNo[clerkId] = seqNo
		sm.kvMap[key] += arg
	}
}

func (sm *KvStateMachine) CheckCommitted(clerkId, seqNo int) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.seqNo[clerkId] >= seqNo
}
