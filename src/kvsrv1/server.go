package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KeyValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	Map map[string]KeyValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.Map = make(map[string]KeyValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	// Your code here.
	value, exists := kv.Map[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		reply.Value = ""
		reply.Version = 0
	} else {
		reply.Value = value.Value
		reply.Version = value.Version
		reply.Err = rpc.OK
	}
	kv.mu.Unlock()
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	//这里需要加锁 因为cas需要是一个原子操作
	kv.mu.Lock()
	value, exist := kv.Map[args.Key]
	reply.Err = rpc.OK
	if !exist {
		if args.Version == 0 {
			kv.Map[args.Key] = KeyValue{
				Value:   args.Value,
				Version: 1,
			}
		} else {
			reply.Err = rpc.ErrNoKey
		}

	} else {
		//锁加在这里，锁力度小一点
		//kv.mu.Lock()
		if value.Version == args.Version {
			kv.Map[args.Key] = KeyValue{
				Value:   args.Value,
				Version: args.Version + 1,
			}
		} else {
			//	版本号不一致 错误
			reply.Err = rpc.ErrVersion
		}

	}
	// Your code here.
	kv.mu.Unlock()
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
