package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	votedFor     int
	logs         []LogEntry
	commitIndex  int
	lastApplied  int
	voteNum      int
	restartTimer bool
	state        string
	leaderState  Leader
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Leader struct {
	NextIndex  []int
	MatchIndex []int
}

//AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int

	Entries []LogEntry
}

//AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = false
	if rf.state == "leader" {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	peers        []*labrpc.ClientEnd
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Ret         bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		rf.BecomeFollower(args.Term)
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		reply.Ret = false
		return
	} else if rf.votedFor == -1 && rf.state == "follower" {

		//**Note that you cannot vote repeatedly**

		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term

		if args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= lastLogIndex {
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateID
			reply.Ret = true
			go rf.ConvertToCandidate(args.peers, rf.me, rf.votedFor, false)
			DPrintf("RequestVote() term:%v serverid:%v, vote for :%v\n", reply.Term, rf.me, args.CandidateID)
			return
		}
	} else {
		reply.VoteGranted = false
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Ret = true
		return
	}
}

//AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("AppendEntries leaderterm:%v, leaderid:%v, serverterm:%v, serverid:%v\n",
		args.Term, args.LeaderID, rf.currentTerm, rf.me)
	//implementation 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//reset heartbeat
	rf.restartTimer = true

	rf.currentTerm = args.Term
	if rf.state != "follower" {
		rf.state = "follower"
		rf.votedFor = -1
		rf.voteNum = 0
	}

	//now all recivers are followers
	//implementation 2
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("rf.logs[args.PrevLogIndex].Term:%v, args.PrevLogTerm:%v\n",
			rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//not heartbeat
	if len(args.Entries) > 0 {
		for i, v := range args.Entries {
			currentIndex := args.PrevLogIndex + i + 1
			if currentIndex < len(rf.logs) && rf.logs[currentIndex].Term != v.Term {
				rf.logs[currentIndex] = v
				rf.logs = rf.logs[:(currentIndex + 1)]
			} else if currentIndex >= len(rf.logs) {
				rf.logs = append(rf.logs, v)
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)
			rf.commitIndex = min(args.LeaderCommit, indexOfLastNewEntry)
		}
	}
	DPrintf("AppendEntries heartbeat or entries\n")
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	DPrintf("Make()\n")
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = -1
	rf.votedFor = -1

	//first index is 1
	rf.logs = make([]LogEntry, 1)
	rf.state = "follower"

	DPrintf("Make() raft term:%v serverid:%v, state:%q\n", rf.currentTerm, me, rf.state)

	go rf.DealRequestVote(peers, me)
	go rf.PrintRaftInfo()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) PrintRaftInfo() {
	for {
		rf.mu.Lock()
		DPrintf("ServerID:%v, state:%q, term:%v, votefor:%v, votenum:%v\n",
			rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.voteNum)
		rf.mu.Unlock()
		time.Sleep(1000 * time.Millisecond)
	}
}

func (rf *Raft) DealRequestVote(peers []*labrpc.ClientEnd, me int) {
	//random from 200 to 400,election timer from 2000ms to 4000ms
	//**heartbeat for 100ms,so election timer must far more than 100ms**
	for {
		electionTimer := rand.Int()%200 + 200
		i := 0
		for ; i < electionTimer; i++ {
			time.Sleep(10 * time.Millisecond)
			restart := false
			rf.mu.Lock()
			if rf.restartTimer == true {
				rf.restartTimer = false
				restart = true
			}
			rf.mu.Unlock()
			if restart == true {
				break
			}
		}

		//timeout
		rf.mu.Lock()
		if i >= electionTimer && rf.state != "leader" {
			addTerm := false
			if rf.state == "follower" {
				DPrintf("DealRequestVote() timeout raft term:%v, serverid:%v, state:%q, ConvertToCandidate\n",
					rf.currentTerm, me, rf.state)

				//only from follower convert to candidate, increment currentTerm
				addTerm = true
			} else {
				DPrintf("Candidate:%v election timeout, start new election\n", rf.me)
			}
			go rf.ConvertToCandidate(peers, me, me, addTerm)

		}
		rf.mu.Unlock()

	}

}

func (rf *Raft) ConvertToCandidate(peers []*labrpc.ClientEnd, me int, voteFor int, addTerm bool) {

	rf.mu.Lock()
	rf.state = "candidate"
	if addTerm == true {
		rf.currentTerm++
	}

	rf.votedFor = voteFor
	if voteFor == me {
		rf.voteNum = 1
	}

	//restart a timer
	rf.restartTimer = true

	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term

	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm, peers}

	DPrintf("ConvertToCandidate() raft term:%v serverid:%v, state:%q\n", rf.currentTerm, me, rf.state)
	rf.mu.Unlock()

	for index, peer := range peers {
		if index != me {
			go func(args *RequestVoteArgs, peer *labrpc.ClientEnd, index int) {
				reply := RequestVoteReply{}
				//begintime := time.Now().Unix()
				//flag := rand.Int31n(100000)
				//DPrintf("begin:%v Candidate Call RequestVote %v to %v. rand:%v\n", begintime, me, index, flag)

				//maybe block, if rpc network wrong
				peer.Call("Raft.RequestVote", args, &reply)

				//endtime := time.Now().Unix()
				//diff := endtime - begintime
				//DPrintf("end:%v diff:%v Candidate Call RequestVote %v to %v rand:%v\n", endtime, diff, me, index, flag)

				rf.mu.Lock()
				if args.Term == rf.currentTerm && rf.state == "candidate" &&
					reply.Ret == true && reply.VoteGranted == true {
					rf.voteNum++
					if rf.voteNum > len(peers)/2 {
						go rf.BecomeLeader(peers, len(peers))
					}
				} else if args.Term == rf.currentTerm && reply.Ret == false &&
					reply.Term > rf.currentTerm && rf.state == "candidate" {
					rf.BecomeFollower(reply.Term)
				}
				rf.mu.Unlock()
			}(&args, peer, index)
		}

	}

}

func (rf *Raft) BecomeLeader(peers []*labrpc.ClientEnd, serverNum int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = "leader"
	rf.votedFor = -1
	rf.voteNum = 0
	rf.leaderState.NextIndex = make([]int, serverNum)
	rf.leaderState.MatchIndex = make([]int, serverNum)
	lastIndex := len(rf.logs)
	for i := 0; i < serverNum; i++ {
		rf.leaderState.NextIndex[i] = lastIndex
	}
	DPrintf("BecomeLeader() raft term:%v serverid:%v, state:%q\n", rf.currentTerm, rf.me, rf.state)
	//send initial empty AppendEntries RPCs
	//(heartbeat) to each server
	go rf.SendHeartBeat(peers)
}

func (rf *Raft) BecomeFollower(term int) {
	rf.state = "follower"
	rf.currentTerm = term
	rf.votedFor = -1
	rf.voteNum = 0
	//restart a timer
	rf.restartTimer = true
	DPrintf("BecomeFollower() raft term:%v serverid:%v, state:%q\n", rf.currentTerm, rf.me, rf.state)
}

func (rf *Raft) SendHeartBeat(peers []*labrpc.ClientEnd) {
	//make empty entries
	entries := make([]LogEntry, 0)
	for {
		rf.mu.Lock()
		if rf.state != "leader" {
			rf.mu.Unlock()
			break
		}

		go rf.SendAppendEntriesRPC(peers, entries)
		DPrintf("SendHeartBeat() raft serverid:%v, state:%q\n", rf.me, rf.state)
		rf.mu.Unlock()

		//heartbeat 100ms
		time.Sleep(100 * time.Millisecond)

	}
}

func (rf *Raft) SendAppendEntriesRPC(peers []*labrpc.ClientEnd, entries []LogEntry) {
	me := rf.me
	DPrintf("SendAppendEntriesRPC() raft serverid:%v\n", rf.me)
	for serverId, peer := range peers {
		if serverId != me {
			go func(peer *labrpc.ClientEnd, serverId int) {
				rf.mu.Lock()
				if rf.state != "leader" {
					rf.mu.Unlock()
					return
				}
				args := AppendEntriesArgs{}
				args.LeaderCommit = rf.commitIndex
				args.LeaderID = rf.me
				args.Term = rf.currentTerm
				args.Entries = entries
				args.PrevLogIndex = rf.leaderState.NextIndex[serverId] - 1

				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term

				rf.mu.Unlock()

				//begintime := time.Now().Unix()
				//flag := rand.Int31n(200000)
				//DPrintf("begin:%v SendAppendEntriesRPC() call %v to %v. rand:%v\n", begintime, me, serverId, flag)

				//next tow line codes not lock , because if call delay too long
				//the full rf mutex would be blocked, cause not to receive RequestVote rpc
				reply := AppendEntriesReply{-1, false}
				peer.Call("Raft.AppendEntries", &args, &reply)

				//endtime := time.Now().Unix()
				//diff := endtime - begintime
				//DPrintf("end:%v diff:%v SendAppendEntriesRPC() call AppendEntries %v to %v. rand:%v\n", endtime, diff, me, serverId, flag)

				//rf.mu.Lock()
				//DPrintf("call AppendEntries raft term:%v leaderid:%v serverid:%v rf.state:%q\n", args.Term, me, serverId, rf.state)
				//DPrintf("PrevLogIndex:%v, PrevLogTerm:%v replysuccess:%v\n", args.PrevLogIndex, args.PrevLogTerm, reply.Success)
				//rf.mu.Unlock()

				retryNum := 0
				for reply.Success == false {

					//when reply term > currentTerm
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.BecomeFollower(reply.Term)
						rf.mu.Unlock()
						break
					} else {

						if reply.Term > -1 && rf.leaderState.NextIndex[serverId] > 1 {

							//nextindex--
							rf.leaderState.NextIndex[serverId]--

							args.PrevLogIndex = rf.leaderState.NextIndex[serverId] - 1
							DPrintf("PrevLogIndex:%v reply.Term:%v success:%v\n",
								args.PrevLogIndex, reply.Term, reply.Success)
							args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
						} else {
							retryNum++
						}
						rf.mu.Unlock()
						if retryNum > 10 {
							break
						}
						reply = AppendEntriesReply{-1, false}

						//maybe blocked too
						peer.Call("Raft.AppendEntries", &args, &reply)

					}
				}
				//this stage is follower, return
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				if retryNum <= 10 {
					rf.leaderState.NextIndex[serverId] += len(entries)
					rf.leaderState.MatchIndex[serverId] = min(rf.leaderState.NextIndex[serverId]-1, 0)
				} else {
					DPrintf("SendAppendEntries from :%v to :%v fail...\n", rf.me, serverId)
				}

				rf.mu.Unlock()
			}(peer, serverId)
		}
	}
	//the last rule for leader
	//TODO

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
