package common

import (
	"crypto/rand"
	"fmt"
	"hash/crc64"
	"math/big"
	"net/rpc"
)

const NShards = 16

func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func Key2Hash(key string) uint8 {
	return uint8(crc64.Checksum([]byte(key), crc64.MakeTable(crc64.ECMA)) % (1 << 8))
}

//
// which shard is a key in?
//
func Key2Shard(key string) int {
	shard := (int(Key2Hash(key)) * NShards / (1 << 8))
	return int(shard)
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
func Call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
