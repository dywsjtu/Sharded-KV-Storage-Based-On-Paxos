package shardkv

import (
	"fmt"
	"time"

	"umich.edu/eecs491/proj5/common"
)

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters
//
type Op struct {
	ID                int64
	ClientID          string
	RequestID         int64
	RequestType       string
	Key               string
	Value             string
	Put               bool
	Shard             int
	Servers           []string
	TransferDataID    int64
	TransferData      *Shard
	TranferDataLeader int
}

type Shard struct {
	Data           map[string]string
	MostRecentDone map[string](int64)
	Own            bool
}

func InitShard() *Shard {
	shard := new(Shard)
	shard.Data = make(map[string]string)
	shard.MostRecentDone = make(map[string]int64)
	shard.Own = false
	return shard
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
	op1 := v1.(Op)
	op2 := v2.(Op)
	if op1.RequestType == op2.RequestType {
		if op1.RequestType == "Get" {
			return op1.Key == op2.Key && op1.ID == op2.ID
		} else if op1.RequestType == "PutAppend" {
			return op1.Key == op2.Key && op1.ClientID == op2.ClientID && op1.RequestID == op2.RequestID
		} else {
			return op1.TransferDataID == op2.TransferDataID
		}
	}
	return false
}

//
// additions to ShardKV state
//
type ShardKVImpl struct {
	Data           map[int]*Shard
	IsTransferDone map[int64]bool
	IsReceiveDone  map[int64]bool
}

//
// initialize kv.impl.*
//
func (kv *ShardKV) InitImpl() {
	kv.impl.IsTransferDone = make(map[int64]bool)
	kv.impl.IsReceiveDone = make(map[int64]bool)
	kv.impl.Data = make(map[int]*Shard)
	for i := 0; i < common.NShards; i++ {
		kv.impl.Data[i] = InitShard()
	}
}

//
// RPC handler for client Get requests
//
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	fmt.Printf("Lock get\n")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Printf("after Lock get\n")
	kv.rsm.AddOp(Op{ID: common.Nrand(), RequestType: "Get", Key: args.Key})
	shard := common.Key2Shard(args.Key)
	if !kv.impl.Data[shard].Own {
		reply.Value = ""
		reply.Err = ErrWrongGroup
	} else {
		if _, exist := kv.impl.Data[shard].Data[args.Key]; !exist {
			reply.Value = ""
			reply.Err = ErrNoKey
		} else {
			reply.Value = kv.impl.Data[shard].Data[args.Key]
			reply.Err = OK
		}
	}
	return nil
}

//
// RPC handler for client Put and Append requests
//
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	fmt.Printf("Lock put\n")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Printf("after Lock put\n")
	kv.rsm.AddOp(Op{RequestType: "PutAppend", Key: args.Key, Value: args.Value, Put: args.Op == "Put", RequestID: args.Impl.RequestID, ClientID: args.Impl.ClientID})
	shard := common.Key2Shard(args.Key)
	if !kv.impl.Data[shard].Own {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
	}
	return nil
}

func (kv *ShardKV) TransferData(args *common.TranferDataArgs, reply *common.TranferDataReply) error {
	fmt.Printf(" Lock transfer data\n")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Printf("after transfer data\n")
	kv.rsm.AddOp(Op{RequestType: "TransferData", TransferDataID: args.ID, Shard: args.Shard, Servers: args.Servers, TranferDataLeader: kv.me})
	return nil
}

func (kv *ShardKV) ReceiveData(args *ReceiveDataArgs, reply *ReceiveDataReply) error {
	fmt.Printf("Lock Receive data\n")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Printf("After Lock Receive data\n")
	kv.rsm.AddOp(Op{RequestType: "ReceiveData", TransferDataID: args.ID, Shard: args.Shard, TransferData: args.Data})
	return nil
}

//
// Execute operation encoded in decided value v and update local state
//
func (kv *ShardKV) ApplyOp(v interface{}) {
	op := v.(Op)
	if op.RequestType == "PutAppend" {
		kv.ApplyPutAppend(op)
	} else if op.RequestType == "TransferData" {
		kv.ApplyTransferData(op)
	} else if op.RequestType == "ReceiveData" {
		kv.ApplyReceiveData(op)
	}
}

//
// Add RPC handlers for any other RPCs you introduce
//
func (kv *ShardKV) ApplyPutAppend(op Op) {
	shard := kv.impl.Data[common.Key2Shard(op.Key)]
	if !shard.Own {
		return
	}
	if _, exist := shard.MostRecentDone[op.ClientID]; !exist {
		shard.MostRecentDone[op.ClientID] = -1
	}
	MostRecentDone := shard.MostRecentDone[op.ClientID]
	if op.RequestID < MostRecentDone {
		return
	} else if op.RequestID == MostRecentDone {
		return
	} else {
		if op.Put {
			shard.Data[op.Key] = op.Value
		} else {
			shard.Data[op.Key] += op.Value
		}
		shard.MostRecentDone[op.ClientID] = op.RequestID
	}
}

func (kv *ShardKV) ApplyTransferData(op Op) {
	if _, exist := kv.impl.IsTransferDone[op.TransferDataID]; exist {
		return
	}

	if op.Shard == common.NShards {
		for i := 0; i < common.NShards; i++ {
			kv.impl.Data[i].Own = true
		}
	} else {
		tempShard := kv.impl.Data[op.Shard]
		kv.impl.Data[op.Shard].Own = false
		if kv.me == op.TranferDataLeader {
			args := &ReceiveDataArgs{op.TransferDataID, op.Shard, tempShard}
			reply := &ReceiveDataReply{}
			for {
				for _, server := range op.Servers {
					// fmt.Printf("I am here %v, %v, %v\n", server, kv.myaddr, len(op.Servers))
					ok := common.Call(server, "ShardKV.ReceiveData", args, reply)
					if ok {
						kv.impl.IsTransferDone[op.TransferDataID] = true
						return
					} else {
						time.Sleep(100 * time.Millisecond)
					}
				}

			}
		}
	}
}

func (kv *ShardKV) ApplyReceiveData(op Op) {
	if _, exist := kv.impl.IsReceiveDone[op.TransferDataID]; exist {
		return
	}
	kv.impl.IsReceiveDone[op.TransferDataID] = true
	kv.impl.Data[op.Shard] = op.TransferData
	kv.impl.Data[op.Shard].Own = true
}
