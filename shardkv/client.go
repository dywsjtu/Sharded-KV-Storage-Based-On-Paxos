package shardkv

import (
	"sync"

	"umich.edu/eecs491/proj5/shardmaster"
)

type Clerk struct {
	mu     sync.Mutex
	sm     *shardmaster.Clerk
	impl   ClerkImpl
}

func MakeClerk(shardmasters []string) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(shardmasters)
	ck.InitImpl()
	return ck
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
