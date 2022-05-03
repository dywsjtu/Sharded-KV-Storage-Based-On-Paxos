package common

//
// define here any data types that you need to access in two packages without
// creating circular dependencies
//
type TranferDataArgs struct {
	ID      int64
	Shard   int
	Servers []string
}

type TranferDataReply struct {
}
