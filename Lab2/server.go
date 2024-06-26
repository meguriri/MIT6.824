package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.RWMutex
	// Your definitions here.
	ma      map[string]string
	checker sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	reply.Value = kv.ma[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Type == Del {
		kv.checker.Delete(args.Id)
		return
	}

	k, v, id := args.Key, args.Value, args.Id

	if _, ok := kv.checker.Load(id); ok {
		return
	}

	kv.mu.Lock()
	kv.ma[k] = v
	kv.mu.Unlock()

	kv.checker.Store(id, v)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Type == Del {
		kv.checker.Delete(args.Id)
		return
	}

	k, v, id := args.Key, args.Value, args.Id

	if ov, ok := kv.checker.Load(id); ok {
		reply.Value = ov.(string)
		return
	}

	kv.mu.Lock()
	oldV := kv.ma[k]
	kv.ma[k] = oldV + v
	reply.Value = oldV
	kv.mu.Unlock()

	kv.checker.Store(id, oldV)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.mu = sync.RWMutex{}
	kv.ma = make(map[string]string)
	kv.checker = sync.Map{}
	return kv
}
