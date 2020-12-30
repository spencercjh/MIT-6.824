package mr

import (
	"fmt"
	"log"
)

const LOG_DEBUG_LEVEL = true

func LogDebug(format string, v ...interface{}) {
	if LOG_DEBUG_LEVEL {
		log.Printf(format+"\n", v...)
	}
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
