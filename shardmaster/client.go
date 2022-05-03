package shardmaster

//
// Shardmaster clerk
//

import (
	"time"

	"umich.edu/eecs491/proj5/common"
)

type Clerk struct {
	servers []string // shardmaster replicas
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num: num}
	var reply QueryReply

	for {
		// try each known server
		for _, srv := range ck.servers {
			ok := common.Call(srv, "ShardMaster.Query", &args, &reply)
			if ok {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(gid int64, servers []string) {
	args := JoinArgs{GID: gid, Servers: servers}
	var reply JoinReply

	for {
		// try each known server
		for _, srv := range ck.servers {
			ok := common.Call(srv, "ShardMaster.Join", &args, &reply)
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gid int64) {
	args := LeaveArgs{GID: gid}
	var reply LeaveReply

	for {
		// try each known server
		for _, srv := range ck.servers {
			ok := common.Call(srv, "ShardMaster.Leave", &args, &reply)
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int64) {
	args := MoveArgs{Shard: shard, GID: gid}
	var reply MoveReply

	for {
		// try each known server
		for _, srv := range ck.servers {
			ok := common.Call(srv, "ShardMaster.Move", &args, &reply)
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
