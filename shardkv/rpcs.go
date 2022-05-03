package shardkv

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Impl  PutAppendArgsImpl
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key  string
	Impl GetArgsImpl
}

type GetReply struct {
	Err   Err
	Value string
}
