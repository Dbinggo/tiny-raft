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
func randomElectionTimeout() time.Time {
	return time.Now().Add(time.Duration(150+rand.Intn(150)) * time.Millisecond)

}
