package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term          int
	isLeader      bool
	votedFor      int
	lastHeartbeat time.Time
	state         string // "follower", "candidate", "leader"
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.term
	isleader = rf.isLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}
type AppendEntries struct {
	Term     int
	LeaderId int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Println(rf.me, "收到", args.CandidateId, "投票请求")
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeartbeat = time.Now()
	if rf.state == "leader" && rf.term < args.Term {
		rf.isLeader = false
		rf.term = args.Term
		rf.votedFor = args.CandidateId
		fmt.Println(rf.me, "同意投票", args.CandidateId, rf.me, "的term是", rf.term, "候选人term是", args.Term)
		return
	}
	if rf.term > args.Term {
		//	如何符合上面这三种条件，直接投反对票
		reply.VoteGranted = false
		fmt.Println(rf.me, "不同意投票", args.CandidateId, "原因rf.term > args.Term", rf.term, args.Term)
		return
	}
	if rf.term == args.Term && rf.votedFor != -1 {
		//	如何符合上面这三种条件，直接投反对票
		reply.VoteGranted = false
		fmt.Println(rf.me, "不同意投票", args.CandidateId, "原因rf.term == args.Term && rf.votedFor != -1")
		return
	}
	reply.VoteGranted = true
	rf.term = args.Term
	rf.votedFor = args.CandidateId
	fmt.Println(rf.me, "同意投票", args.CandidateId, rf.me, "的term是", rf.term, "候选人term是", args.Term)
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//候选者处理每个请求的回复

	return ok
}

func (rf *Raft) Heartbeat(args *AppendEntries, reply *AppendEntriesReply) {
	fmt.Println(rf.me, "收到心跳心跳term", rf.term)
	if args.Term >= rf.term {
		if args.Term > rf.term && rf.isLeader {
			rf.isLeader = false
			rf.state = "follower"
		}
		//这里有选举问题， 比如出现选取前2。选举后5
		rf.mu.Lock()
		rf.lastHeartbeat = time.Now()
		rf.term = args.Term
		rf.mu.Unlock()
		reply.Success = true
		return
	}
	reply.Success = false
	return
}
func (rf *Raft) sendHeartbeat(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.Heartbeat", args, reply)

	return ok

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 定时心跳
func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		//多久时间发送一次心跳
		//ms := 50 + (rand.Int63() % 300)
		ms := 200
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if rf.killed() == false && rf.isLeader {
			for index := range rf.peers {
				fmt.Println(rf.me, "开始心跳term", rf.term, rf.isLeader)
				if index == rf.me {
					continue
				}
				go func(peer int) {
					args := AppendEntries{
						Term:     rf.term,
						LeaderId: rf.me,
					}
					reply := AppendEntriesReply{}
					rf.sendHeartbeat(index, &args, &reply)

				}(index)
			}
		}

	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		ms := 300 + (rand.Int63() % 300)
		timeoutDuration := time.Duration(ms) * time.Millisecond
		rf.mu.Lock()
		lastHeartbeat := rf.lastHeartbeat
		rf.mu.Unlock()

		// 每 10ms 检查一次是否超时
		for rf.killed() == false && time.Since(lastHeartbeat) < timeoutDuration {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			lastHeartbeat = rf.lastHeartbeat
			rf.mu.Unlock()
		}
		//var lock sync.Mutex
		if rf.killed() == false && !rf.isLeader {
			//lock.Lock()
			rf.mu.Lock()
			rf.state = "candidate"
			fmt.Println(rf.me, "选举前term", rf.term, rf.isLeader)
			rf.term++
			rf.votedFor = rf.me
			//	发送选举
			nowTerm := rf.term
			fmt.Println(rf.me, "选举前后", rf.term, rf.isLeader)
			voteNum := 1
			rf.mu.Unlock()
			var wg sync.WaitGroup
			//mu := sync.Mutex{}
			fmt.Println(rf.me, "开始选举:")
			for index := range rf.peers {
				if index == rf.me {
					continue
				}
				wg.Add(1)
				go func(peer int) {
					defer wg.Done() // 减少计数
					args := RequestVoteArgs{
						Term:        rf.term,
						CandidateId: rf.me,
					}
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(index, &args, &reply)
					if ok {
						rf.mu.Lock()
						if reply.VoteGranted {
							voteNum++
							fmt.Println(voteNum, len(rf.peers)/2, "是否大于需要票数")
							fmt.Println("是否“”", nowTerm == rf.term)
							if voteNum > len(rf.peers)/2 && nowTerm == rf.term && rf.state != "leader" {
								rf.isLeader = true
								rf.state = "leader"
								fmt.Println(rf.me, "提前成为领导")

							}
						}
						rf.mu.Unlock()
					}
				}(index)

				//if voteNum > len(rf.peers)/2 {
				//	rf.isLeader = true
				//	fmt.Println(rf.me, "成为领导")
				//}
			}
			wg.Wait()
			rf.mu.Lock()
			if voteNum > len(rf.peers)/2 && nowTerm == rf.term {
				if rf.state == "follower" {
					fmt.Println(rf.me, "同意票数:", voteNum)
					fmt.Println(rf.me, "一半要:", len(rf.peers)/2)
					rf.isLeader = true
					rf.state = "leader"
					fmt.Println(rf.me, "成为领导")
				}

			} else {
				rf.state = "follower"
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.lastHeartbeat = time.Now()
	rf.term = 1
	rf.isLeader = false
	rf.state = "follower"
	rf.votedFor = -1
	fmt.Println(rf.me, "重启")
	fmt.Println(rf.me, "重启后是不是领导？", rf.isLeader)
	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()
	return rf
}
