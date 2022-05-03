# Sharded-KV-Storage-Based-On-Paxos
This project is a implementation of distributed, sharded, replicated KV storage system 
which supports `Put`, `Append`, and `Get` operations and provides linearizability. 
<br/>
<br/>
`paxos` and `paxosrsm` implemented the paxos protocol and a replicated state machine(RSM) based on paxos.
<br/>
<br/>
`shardmaster` implemented a replicated shard master server managing the replica groups of servers, handling died and joined servers and doing the load-balancing among groups. 
<br/>
<br/>
`shardkv` implemented a shard server operating as part of a replica group, in which every server can serve concurrent requests.
<br/>
<br/>
The `shardmaster` provides a distributed hash table. Each replica group will be responsible for several shards. The whole system is fault-tolerant as long as the majority of a replica group or shard masters lives.


