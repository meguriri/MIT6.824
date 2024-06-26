package kvsrv

// Put or Append
const (
	Op = iota
	Del
)

type PutAppendArgs struct {
	Id    int64
	Key   string
	Value string
	Type  int8
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Id  int64
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
