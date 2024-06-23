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
	ms := 700 + rand.Int63()%400

	return time.Duration(ms) * time.Millisecond
}

func RandomizedHeartbeatTime() time.Duration {
	// TODO: proper duration with randomization
	ms := 300
	return time.Duration(ms) * time.Millisecond
}
