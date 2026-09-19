package rpc

type Err string

const (
	// 服务端与 Clerk 均可能返回的错误。
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// 仅由 Clerk 返回，表示写入结果无法确定。
	ErrMaybe = "ErrMaybe"

	// 后续 kvraft 实验使用的错误。
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

type TValue struct {
	Value   string
	Version Tversion
}
