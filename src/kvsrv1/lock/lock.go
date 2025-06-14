package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck  kvtest.IKVClerk
	id  string
	key string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.id = kvtest.RandValue(8)
	lk.key = l
	return lk
}

func (lk *Lock) Acquire() {
	//func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err)
	//func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			err := lk.ck.Put(lk.key, lk.id, 0)
			if err == rpc.OK {
				return
			} else if err == rpc.ErrMaybe {
				//	检查是否已经加成功了
				value, _, _ := lk.ck.Get(lk.key)
				if value == lk.id {
					return
				}
			}
			//	如果加锁成功 就返回，否则继续自转重试
		} else if value == "" {
			err := lk.ck.Put(lk.key, lk.id, version)
			if err == rpc.OK {
				return
			} else if err == rpc.ErrMaybe {
				//	检查是否已经加成功了
				value, _, _ := lk.ck.Get(lk.key)
				if value == lk.id {
					return
				}
			}
		}
	}
}

func (lk *Lock) Release() {
	//释放锁
	// Your code here
	value, version, err := lk.ck.Get(lk.key)
	if err == rpc.ErrNoKey || lk.id != value {
		return
	}

	lk.ck.Put(lk.key, "", version)
}
