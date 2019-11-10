package raft

import "log"
import "fmt"
import "time"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Log(format string, args ...interface{}) {
  nowStr := time.Now().Format("15:04:05.000")
  s := fmt.Sprintf("%s LOG: ", nowStr)
  s += fmt.Sprintf(format, args...)
  fmt.Printf("%s", s)
}
