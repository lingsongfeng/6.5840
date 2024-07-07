package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func RandomizedElectionTime() time.Duration {
	// TODO: proper duration with randomization
	ms := 800 + rand.Int63()%200

	return time.Duration(ms) * time.Millisecond
}

func RandomizedHeartbeatTime() time.Duration {
	// TODO: proper duration with randomization
	ms := 150
	return time.Duration(ms) * time.Millisecond
}

const ExpBackoffFactor = 2

func SortIntDesc(arr []int) {
	n := len(arr)
	for i := 1; i < n; i++ {
		for j := 0; j < i; j++ {
			if arr[i] > arr[j] {
				arr[i], arr[j] = arr[j], arr[i]
			}
		}
	}
}
