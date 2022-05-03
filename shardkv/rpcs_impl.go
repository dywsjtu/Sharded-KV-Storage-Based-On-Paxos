package shardkv

// Field names must start with capital letters,
// otherwise RPC will break.

//
// additional state to include in arguments to PutAppend RPC.
//
type PutAppendArgsImpl struct {
	ClientID  string
	RequestID int64
}

//
// additional state to include in arguments to Get RPC.
//
type GetArgsImpl struct {
}

//
// for new RPCs that you add, declare types for arguments and reply
//

type ReceiveDataArgs struct {
	ID    int64
	Shard int
	Data  *Shard
}

type ReceiveDataReply struct {
}
