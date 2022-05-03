package shardmaster

import (
	"time"

	"umich.edu/eecs491/proj5/common"
)

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
//
type Op struct {
	ID        int64
	GID       int64
	Shard     int
	Type      string
	Servers   []string
	ConfigNum int
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
	op1 := v1.(Op)
	op2 := v2.(Op)
	if op1.ID != op2.ID || op1.Type != op2.Type {
		return false
	} else {
		if op1.Type == "Join" {
			return op1.GID == op2.GID
		} else if op1.Type == "Leave" {
			return op1.GID == op2.GID
		} else if op1.Type == "Move" {
			return op1.GID == op2.GID && op1.Shard == op2.Shard
		} else if op1.Type == "Query" {
			return op1.ConfigNum == op2.ConfigNum
		}
	}
	return false
}

//
// additions to ShardMaster state
//
type ShardMasterImpl struct {
}

//
// initialize sm.impl.*
//
func (sm *ShardMaster) InitImpl() {
}

//
// RPC handlers for Join, Leave, Move, and Query RPCs
//
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	operation := Op{common.Nrand(), args.GID, -1, "Join", args.Servers, -1}
	sm.rsm.AddOp(operation)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	operation := Op{common.Nrand(), args.GID, -1, "Leave", nil, -1}
	sm.rsm.AddOp(operation)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	operation := Op{common.Nrand(), args.GID, args.Shard, "Move", nil, -1}
	sm.rsm.AddOp(operation)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	operation := Op{common.Nrand(), -1, -1, "Move", nil, args.Num}
	sm.rsm.AddOp(operation)
	if args.Num <= -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	return nil
}

func (sm *ShardMaster) ApplyJoin(op Op) {
	if op.GID <= 0 {
		return
	}
	if _, exists := sm.configs[len(sm.configs)-1].Groups[op.GID]; exists {
		return
	}
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{}
	newConfig.Num = len(sm.configs)
	newConfig.Groups = make(map[int64][]string)
	if len(sm.configs) == 1 {
		newConfig.Groups[op.GID] = op.Servers
		for i := 0; i < common.NShards; i++ {
			newConfig.Shards[i] = op.GID
		}
		sm.configs = append(sm.configs, newConfig)
		// TODO: transfer data
		sm.TranferData(common.NShards, op.GID, op.GID, op.Servers)
	} else {
		newConfig.Shards = lastConfig.Shards
		newConfig.Groups[op.GID] = op.Servers
		for group, shard := range lastConfig.Groups {
			newConfig.Groups[group] = shard
		}
		group2shard := make(map[int64][]int)
		for shard, gid := range lastConfig.Shards {
			group2shard[gid] = append(group2shard[gid], shard)
		}
		if len(lastConfig.Shards) > len(lastConfig.Groups) { //?
			takenShards := make([]int, 0)
			average := len(lastConfig.Shards) / (len(lastConfig.Groups) + 1)
			balanced := false
			for !balanced {
				flag := false
				for group, shards := range group2shard {
					if len(shards) > average {
						takenShards = append(takenShards, shards[0])
						group2shard[group] = shards[1:]
						flag = true
					}
					if len(takenShards) >= average {
						balanced = true
						flag = false
						break
					}
				}
				if !flag {
					break
				}
			}
			for i := 0; i < len(takenShards); i++ {
				sourceGroupID := lastConfig.Shards[takenShards[i]]
				newConfig.Shards[takenShards[i]] = op.GID
				// TODO: transfer data
				sm.TranferData(takenShards[i], sourceGroupID, op.GID, op.Servers)
			}
		}
		sm.configs = append(sm.configs, newConfig)
	}
}

func (sm *ShardMaster) ApplyLeave(op Op) {
	if op.GID <= 0 {
		return
	}
	if _, exists := sm.configs[len(sm.configs)-1].Groups[op.GID]; !exists {
		return
	}
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{}
	newConfig.Num = len(sm.configs)
	newConfig.Groups = make(map[int64][]string)
	newConfig.Shards = lastConfig.Shards
	group2shard := make(map[int64][]int)
	for group, servers := range lastConfig.Groups {
		if group != op.GID {
			newConfig.Groups[group] = servers
		}
		group2shard[group] = make([]int, 0)
	}

	for shard, gid := range lastConfig.Shards {
		group2shard[gid] = append(group2shard[gid], shard)
	}
	average := len(lastConfig.Shards) / (len(lastConfig.Groups))
	leaveShards := group2shard[op.GID]
	delete(group2shard, op.GID)
	// fmt.Printf("%v, %v, %v\n", lastConfig.Groups, newConfig.Groups, lastConfig.Shards)
	// fmt.Printf("ApplyOp %v, %v, %v, %v\n", newConfig.Shards, average, leaveShards, op.GID)

	for group, shards := range group2shard {
		if len(leaveShards) == 0 {
			break
		}
		if len(shards) <= average {
			newConfig.Shards[leaveShards[0]] = group
			tranferShard := leaveShards[0]
			leaveShards = leaveShards[1:]
			sm.TranferData(tranferShard, op.GID, group, lastConfig.Groups[group])
		}
	}
	flag := false
	for {
		for group, _ := range group2shard {
			if len(leaveShards) == 0 {
				flag = true
				break
			}
			newConfig.Shards[leaveShards[0]] = group
			tranferShard := leaveShards[0]
			leaveShards = leaveShards[1:]
			sm.TranferData(tranferShard, op.GID, group, lastConfig.Groups[group])
		}
		if flag {
			break
		}
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) ApplyMove(op Op) {
	if op.GID <= 0 {
		return
	}
	if _, exists := sm.configs[len(sm.configs)-1].Groups[op.GID]; !exists {
		return
	}
	if sm.configs[len(sm.configs)-1].Shards[op.Shard] == op.GID {
		return
	}
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{}
	newConfig.Num = len(sm.configs)
	newConfig.Groups = make(map[int64][]string)

	newConfig.Shards = lastConfig.Shards
	for group, shard := range lastConfig.Groups {
		newConfig.Groups[group] = shard
	}

	newConfig.Shards[op.Shard] = op.GID
	sm.TranferData(op.Shard, lastConfig.Shards[op.Shard], op.GID, lastConfig.Groups[op.GID])
	sm.configs = append(sm.configs, newConfig)
}

//
// Execute operation encoded in decided value v and update local state
//
func (sm *ShardMaster) ApplyOp(v interface{}) {
	operation := v.(Op)
	if operation.Type == "Join" {
		sm.ApplyJoin(operation)
	} else if operation.Type == "Leave" {
		sm.ApplyLeave(operation)
	} else if operation.Type == "Move" {
		sm.ApplyMove(operation)
	}
}

func (sm *ShardMaster) TranferData(shard int, sourceGroupID int64, destGroupID int64, servers []string) {
	lastConfig := sm.configs[len(sm.configs)-1]
	args := &common.TranferDataArgs{ID: common.Nrand(), Shard: shard, Servers: servers}
	reply := &common.TranferDataReply{}
	for {
		for _, server := range lastConfig.Groups[sourceGroupID] {
			if common.Call(server, "ShardKV.TransferData", args, reply) {
				return
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}

	}
}
