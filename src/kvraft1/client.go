package kvraft

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, leader: 0}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) tryNext(leader int) int {
	length := len(ck.servers)
	leader++
	leader %= length
	return leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	var getArgs rpc.GetArgs
	var getReply rpc.GetReply
	leader := ck.leader
	for {
		getArgs = rpc.GetArgs{Key: key}
		getReply = rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &getArgs, &getReply)

		// 通信失败，可能是请求丢失或者回复丢失，直接重试即可
		if !ok {
			leader = ck.tryNext(leader)
			continue
		}

		switch getReply.Err {
		case rpc.ErrWrongLeader:
			leader = ck.tryNext(leader)
			continue
		case rpc.OK:
			ck.leader = leader
			return getReply.Value, getReply.Version, getReply.Err
		case rpc.ErrNoKey:
			ck.leader = leader
			return "", 0, getReply.Err
		default:
			leader = ck.tryNext(leader)
			continue
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	var putArgs rpc.PutArgs
	var putReply rpc.PutReply
	retry := 0
	leader := ck.leader
	for {
		putArgs = rpc.PutArgs{
			Key:     key,
			Value:   value,
			Version: version,
		}
		putReply = rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &putArgs, &putReply)
		if !ok {
			leader = ck.tryNext(leader)
			retry++
			continue
		}
		switch putReply.Err {
		case rpc.ErrWrongLeader:
			leader = ck.tryNext(leader)
			retry++
			continue
		case rpc.ErrVersion:
			ck.leader = leader
			if retry == 0 {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		case rpc.OK:
			ck.leader = leader
			return rpc.OK
		case rpc.ErrNoKey:
			ck.leader = leader
			return rpc.ErrNoKey
		default:
			leader = ck.tryNext(leader)
			continue
		}

	}
}
