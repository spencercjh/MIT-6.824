package raft

import "log"

const LOG_DEBUG_LEVEL = true

func LogDebug(format string, v ...interface{}) {
	if LOG_DEBUG_LEVEL {
		log.Printf(format+"\n", v...)
	}
}
