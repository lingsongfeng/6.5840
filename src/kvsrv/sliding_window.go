package kvsrv

import (
	"container/list"
)

type SlidingWindow struct {
	deque *list.List
}

type Element struct {
	payload interface{}
	seqNo   int
}

func NewSlidingWindow() *SlidingWindow {
	return &SlidingWindow{
		deque: list.New(),
	}
}

func (sw *SlidingWindow) Relax(ack int) {
	for sw.deque.Len() > 0 && sw.deque.Front().Value.(Element).seqNo < ack {
		sw.deque.Remove(sw.deque.Front())
	}
}

func (sw *SlidingWindow) Insert(payload interface{}, seqNo int) {
	sw.deque.PushBack(Element{payload: payload, seqNo: seqNo})
}

func (sw *SlidingWindow) FindBySeqNo(seqNo int) (interface{}, bool) {
	if sw.deque.Len() > 0 && seqNo < sw.deque.Front().Value.(Element).seqNo {
		return "EXPIRED", true
	}
	for e := sw.deque.Back(); e != nil; e = e.Prev() {
		if e.Value.(Element).seqNo == seqNo {
			return e.Value.(Element).payload, true
		}
	}
	return nil, false
}
