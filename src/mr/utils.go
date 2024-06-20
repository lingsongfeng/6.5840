package mr

import "fmt"

func IntermediateFileName(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func OutFileName(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}
