package raft

import "log"

// 调试输出开关与辅助函数。
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
