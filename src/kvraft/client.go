package kvraft

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("client get key:%v", key)
	args := GetArgs{key}
	res := ""
	flag := false
	for {
		for i := range ck.servers {
			reply := GetReply{}
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok == true && reply.Err == OK {
				res = reply.Value
				DPrintf("Get succ. key:%v, value:%v", key, reply.Value)
				flag = true
				break
			} else {
				//DPrintf("Get fail! key:%v ret:%v", key, reply.Err)
			}
		}
		if true == flag {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

	return res
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op}
	flag := false
	for {
		for i := range ck.servers {
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok == true && reply.Err == OK {
				DPrintf("PutAppend succ. key:%v, value:%v, op:%v", key, value, op)
				flag = true
				break
			} else {
				//DPrintf("PutAppend fail! key:%v value:%v op:%v ret:%v", key, value, op, reply.Err)
			}
		}
		if true == flag {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("client put key:%v value:%v", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("client Append key:%v value:%v", key, value)
	ck.PutAppend(key, value, "Append")
}
