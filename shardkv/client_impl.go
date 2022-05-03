package shardkv

import (
	"strconv"
	"time"

	"umich.edu/eecs491/proj5/common"
)

//
// additions to Clerk state
//
type ClerkImpl struct {
	ClientID  string
	RequestID int64
}

//
// initialize ck.impl.*
//
func (ck *Clerk) InitImpl() {
	ck.impl.ClientID = strconv.Itoa(int(time.Now().UnixNano())) + "-" + strconv.Itoa(int(common.Nrand()))
	ck.impl.RequestID = 0
}

//
// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	shard := common.Key2Shard(key)
	args := &GetArgs{key, GetArgsImpl{}}
	reply := &GetReply{}
	for {
		config := ck.sm.Query(-1)
		servers := config.Groups[config.Shards[shard]]
		for _, server := range servers {
			ok := common.Call(server, "ShardKV.Get", &args, &reply)
			if ok {
				if reply.Err == ErrNoKey || reply.Err == OK {
					return reply.Value
				} else if reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// send a Put or Append request.
// keep retrying forever until success.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	shard := common.Key2Shard(key)
	args := &PutAppendArgs{key, value, op, PutAppendArgsImpl{ck.impl.ClientID, ck.impl.RequestID}}
	reply := &PutAppendReply{}
	for {
		config := ck.sm.Query(-1)
		servers := config.Groups[config.Shards[shard]]
		for _, server := range servers {
			ok := common.Call(server, "ShardKV.PutAppend", &args, &reply)
			if ok {
				if reply.Err == ErrWrongGroup {
					break
				} else if reply.Err == OK {
					ck.impl.RequestID += 1
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
