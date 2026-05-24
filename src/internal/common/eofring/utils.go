package eofring

import "fmt"

func GetInputAndOutputQueueNames(nodeId int, nodesAmount int, eofInputStr, eofOutputStr string) (string, string) {
	next := nodeId + 1

	if nodeId == nodesAmount-1 {
		next = 0
	}

	return fmt.Sprintf("%s_%d", eofInputStr, nodeId), fmt.Sprintf("%s_%d", eofOutputStr, next)
}
