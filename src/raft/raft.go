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
	voteMap      map[int]bool
	restartTimer bool
	state        string
	leaderState  Leader
	applyCh      chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Leader struct {
	NextIndex  []int
	MatchIndex []int
}

//wait most server excute end
type WaitMost struct {
	mu     sync.Mutex
	number int
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
	NextIndex int
	Term      int
	Modify    bool
	Success   bool
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
	} else if rf.votedFor == -1 && rf.state == "follower" ||
		rf.votedFor == args.CandidateID && rf.state == "follower" {

		//**Note that you cannot vote repeatedly**

		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term

		//
		//If votedFor is null or candidateId, and candidate’s log is at
		//least as up-to-date as receiver’s log
		//Raft determines which of two logs is more up-to-date
		//by comparing the index and term of the last entries in the
		//logs. If the logs have last entries with different terms, then
		//the log with the later term is more up-to-date. If the logs
		//end with the same term, then whichever log is longer is
		//more up-to-date
		//
		if args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateID
			reply.Ret = true
			go rf.ConvertToCandidate(args.peers, rf.me, rf.votedFor, false)
			//DPrintf("RequestVote() term:%v serverid:%v, vote for :%v\n", reply.Term, rf.me, args.CandidateID)
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
	DPrintf("AppendEntries leaderterm:%v, leaderid:%v, serverterm:%v, serverid:%v, args.PrevLogIndex:%v, args.PrevLogTerm:%v entriesLen:%v LeaderCommit:%v\n",
		args.Term, args.LeaderID, rf.currentTerm, rf.me, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	//implementation 1

	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
	if rf.state != "follower" {
		rf.state = "follower"
		rf.votedFor = -1
		rf.voteMap = make(map[int]bool)
	}

	//reset heartbeat
	rf.restartTimer = true

	//now all recivers are followers
	//implementation 2
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		overFlow := false
		if args.PrevLogIndex >= len(rf.logs) {
			overFlow = true
		}
		DPrintf("serverid:%v args.PrevLogIndex:%v, args.PrevLogTerm:%v overflow:%v\n",
			rf.me, args.PrevLogIndex, args.PrevLogTerm, overFlow)
		reply.Term = rf.currentTerm
		reply.Modify = true
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

	}
	//DPrintf("AppendEntries heartbeat or entries\n")
	if args.LeaderCommit > rf.commitIndex {
		beginIndex := rf.commitIndex
		indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)

		minCommit := min(args.LeaderCommit, indexOfLastNewEntry)
		rf.commitIndex = max(rf.commitIndex, minCommit)
		for beginIndex++; beginIndex <= rf.commitIndex; beginIndex++ {
			DPrintf("AppendEntries serverid:%v ApplyMsg: Index:%v command:%v", rf.me, beginIndex, rf.logs[beginIndex].Command)
			applyMsg := ApplyMsg{true, rf.logs[beginIndex].Command, beginIndex}
			rf.applyCh <- applyMsg
		}

	}
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied = rf.commitIndex
	}
	DPrintf("serverID:%v add Entries, length:%v, log len:%v commitIndex:%v Term:%v\n", rf.me, len(args.Entries), len(rf.logs), rf.commitIndex, rf.currentTerm)
	DPrintf("AppendEntries serverID:%v logs:%v\n", rf.me, rf.logs)
	reply.NextIndex = len(rf.logs)
	reply.Term = rf.currentTerm
	reply.Modify = true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "leader" {
		isLeader = false
	} else {
		term = rf.currentTerm
		logEntry := make([]LogEntry, 1)
		lastCommond := rf.logs[len(rf.logs)-1].Command

		//if lastCommond == commond re-start, not to append, use last log entry
		if lastCommond != command {
			logEntry[0] = LogEntry{term, command}
			rf.logs = append(rf.logs, logEntry...)

		} else {

			// to avoid repeated commond
			if len(rf.logs)-1 == rf.commitIndex {
				index = len(rf.logs) - 1
				DPrintf("start repeated commond,commitindex:%v\n", rf.commitIndex)
				return index, term, isLeader
			}
			rf.logs[len(rf.logs)-1].Term = term
			logEntry[0] = rf.logs[len(rf.logs)-1]

		}
		index = len(rf.logs) - 1

		DPrintf("Start() term:%v serverID:%v index:%v\n", term, rf.me, index)
		DPrintf("Start serverID:%v logs:%v\n", rf.me, rf.logs)
		go rf.SendAppendEntriesRPC(rf.peers, logEntry, index)
	}

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
	rf.applyCh = applyCh

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
		DPrintf("ServerID:%v, state:%q, term:%v, votefor:%v, votenum:%v commitedIndex:%v log len:%v\n",
			rf.me, rf.state, rf.currentTerm, rf.votedFor, len(rf.voteMap), rf.commitIndex, len(rf.logs))
		rf.mu.Unlock()
		time.Sleep(1000 * time.Millisecond)
	}
}

func (rf *Raft) DealRequestVote(peers []*labrpc.ClientEnd, me int) {
	//random from 50 to 210,election timer from 500ms to 2100ms
	//**heartbeat for 100ms,so election timer must far more than 100ms**
	for {
		electionTimer := int(rand.Int63()%160 + 50)
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
			addTerm := true
			if rf.state == "follower" {
				DPrintf("DealRequestVote() timeout raft term:%v, serverid:%v, state:%q, ConvertToCandidate\n",
					rf.currentTerm, me, rf.state)

				//only from follower convert to candidate, increment currentTerm
				//addTerm = true
			} else {
				//paper 5.2 :When this happens, each
				//candidate will time out and start a new election by incre-
				//menting its term and initiating another round of Request-
				//Vote RPCs.
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
		rf.voteMap = make(map[int]bool)
		rf.voteMap[me] = true
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
					rf.voteMap[index] = true
					if len(rf.voteMap) > len(peers)/2 {
						go rf.BecomeLeader()
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

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = "leader"
	rf.votedFor = -1
	rf.voteMap = make(map[int]bool)
	serverNum := len(rf.peers)
	rf.leaderState.NextIndex = make([]int, serverNum)
	rf.leaderState.MatchIndex = make([]int, serverNum)
	lastIndex := len(rf.logs)
	for i := 0; i < serverNum; i++ {
		rf.leaderState.NextIndex[i] = lastIndex
	}
	DPrintf("BecomeLeader() raft term:%v serverid:%v, state:%q\n", rf.currentTerm, rf.me, rf.state)
	//send initial empty AppendEntries RPCs
	//(heartbeat) to each server
	go rf.SendHeartBeat()
}

func (rf *Raft) BecomeFollower(term int) {
	rf.state = "follower"
	rf.currentTerm = term
	rf.votedFor = -1
	rf.voteMap = make(map[int]bool)
	//restart a timer
	rf.restartTimer = true
	DPrintf("BecomeFollower() raft term:%v serverid:%v, state:%q\n", rf.currentTerm, rf.me, rf.state)
}

func (rf *Raft) SendHeartBeat() {
	//make empty entries
	entries := make([]LogEntry, 0)
	for {
		rf.mu.Lock()
		if rf.state != "leader" {
			rf.mu.Unlock()
			break
		}
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied = rf.commitIndex
		}
		go rf.SendAppendEntriesRPC(rf.peers, entries, rf.commitIndex)
		//DPrintf("SendHeartBeat() raft serverid:%v, state:%q\n", rf.me, rf.state)
		rf.mu.Unlock()

		//heartbeat 100ms
		time.Sleep(100 * time.Millisecond)

	}
}

func (rf *Raft) SendAppendEntriesRPC(peers []*labrpc.ClientEnd, entries []LogEntry, commitedIndex int) {
	me := rf.me
	DPrintf("SendAppendEntriesRPC() raft serverid:%v commiterIndex:%v\n", rf.me, commitedIndex)
	rf.mu.Lock()
	entriesLen := len(entries)
	if commitedIndex < rf.commitIndex || commitedIndex == rf.commitIndex && entriesLen > 0 {
		DPrintf("current commitIndex:%v args commitedIndex:%v entries len:%v.so old AppendEntriesRPC,skip..\n", rf.commitIndex, commitedIndex, entriesLen)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	waitMost := WaitMost{number: 0}
	for serverId, peer := range peers {
		if serverId != me {
			rf.mu.Lock()

			//the result causes next line is that before commit index,start the same commond twice
			if entriesLen > 0 && len(rf.logs)-1 < rf.leaderState.NextIndex[serverId] ||
				entriesLen == 0 && len(rf.logs) < rf.leaderState.NextIndex[serverId] {
				DPrintf("Point of doubt: leaderid:%v serverid:%v leaderTerm:%v leaderCommit:%v commitedIndex:%v last log index:%v, nextIndex:%v ,entriesLen:%v\n", me, serverId, rf.currentTerm, rf.commitIndex, commitedIndex, len(rf.logs)-1, rf.leaderState.NextIndex[serverId], entriesLen)
			}
			if entriesLen == 0 || len(rf.logs)-1 >= rf.leaderState.MatchIndex[serverId] {
				go rf.SendAppendEntriesRPCToOneServer(peer, &waitMost, serverId, entries, commitedIndex)
			} else if rf.leaderState.MatchIndex[serverId] >= commitedIndex {

				waitMost.mu.Lock()
				waitMost.number++
				waitMost.mu.Unlock()
			}

			rf.mu.Unlock()
		}
	}
	//the last rule for leader

	//time.Sleep(500 * time.Millisecond)
	rf.mu.Lock()
	if commitedIndex > rf.commitIndex && rf.state == "leader" {
		//wait for most server excute end
		DPrintf("commitedIndex:%v > rf.commitIndex:%v\n", commitedIndex, rf.commitIndex)
		followerNum := len(peers)
		rf.mu.Unlock()
		for {
			rf.mu.Lock()
			waitMost.mu.Lock()
			if waitMost.number+1 > followerNum/2 &&
				rf.logs[commitedIndex].Term == rf.currentTerm {
				DPrintf("waitMost.number:%v followerNum:%v\n", waitMost.number, followerNum)
				beginIndex := rf.commitIndex
				rf.commitIndex = commitedIndex
				rf.leaderState.NextIndex[me] += len(entries)
				rf.leaderState.MatchIndex[me] = max(rf.leaderState.NextIndex[me]-1, 0)
				for beginIndex++; beginIndex <= rf.commitIndex; beginIndex++ {
					applyMsg := ApplyMsg{true, rf.logs[beginIndex].Command, beginIndex}
					rf.applyCh <- applyMsg
					DPrintf("Update LeaderID:%v commitedIndex:%v\n", rf.me, beginIndex)
				}
				waitMost.mu.Unlock()
				rf.mu.Unlock()
				break
			}
			waitMost.mu.Unlock()
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}

		/**
		matchNum := 0
		for _, v := range rf.leaderState.MatchIndex {
			DPrintf("rf.leaderState.MatchIndex:%v\n", v)
			if v >= commitedIndex {
				matchNum++
			}
		}
		**/

	} else {
		rf.mu.Unlock()
	}

}

func (rf *Raft) SendAppendEntriesRPCToOneServer(peer *labrpc.ClientEnd, waitMost *WaitMost, serverId int, entries []LogEntry, commitedIndex int) {
	rf.mu.Lock()
	if rf.state != "leader" {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{}
	args.LeaderCommit = rf.commitIndex
	args.LeaderID = rf.me
	args.Term = rf.currentTerm
	nextIndex := rf.leaderState.NextIndex[serverId]
	args.PrevLogIndex = nextIndex - 1
	DPrintf("begin args serverID:%v commitIndex:%v nextIndex:%v leadercommit:%v args.PrevLogIndex:%v loglen:%v entriesLen:%v\n",
		serverId, commitedIndex, nextIndex, rf.commitIndex, args.PrevLogIndex, len(rf.logs), len(entries))

	if len(entries) > 0 {
		if nextIndex > commitedIndex {
			//in the same time ues start() twice,when bigger one end,litter one begin,case this situation
			DPrintf("Point of doubt too: leaderid:%v serverid:%v leaderTerm:%v leaderCommit:%v commitedIndex:%v last log index:%v, nextIndex:%v\n", rf.me, serverId, rf.currentTerm, rf.commitIndex, commitedIndex, len(rf.logs)-1, nextIndex)
			args.PrevLogIndex = commitedIndex - 1
			args.Entries = entries
		} else {
			args.Entries = rf.logs[nextIndex : commitedIndex+1]
		}

	} else {
		if args.PrevLogIndex >= len(rf.logs) {
			args.PrevLogIndex = len(rf.logs) - 1
		}
		args.Entries = entries
	}
	args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	rf.mu.Unlock()

	//begintime := time.Now().Unix()
	//flag := rand.Int31n(10000)
	//DPrintf("begin:%v SendAppendEntriesRPCToOneServer() call %v to %v. rand:%v\n", begintime, rf.me, serverId, flag)

	//next tow line codes not lock , because if call delay too long
	//the full rf mutex would be blocked, cause not to receive RequestVote rpc
	reply := AppendEntriesReply{}
	peer.Call("Raft.AppendEntries", &args, &reply)

	//endtime := time.Now().Unix()
	//diff := endtime - begintime
	//DPrintf("end:%v diff:%v SendAppendEntriesRPCToOneServer() modify:%v call AppendEntries %v to %v. reply Term:%v success:%v rand:%v\n", endtime, diff, reply.Modify, rf.me, serverId, reply.Term, reply.Success, flag)

	//rf.mu.Lock()
	//DPrintf("call AppendEntries raft term:%v leaderid:%v serverid:%v rf.state:%q\n", args.Term, me, serverId, rf.state)
	//DPrintf("PrevLogIndex:%v, PrevLogTerm:%v replysuccess:%v\n", args.PrevLogIndex, args.PrevLogTerm, reply.Success)
	//rf.mu.Unlock()

	//5.3 Log replication
	convertFollower := false

	nextIndexTmp := args.PrevLogIndex + 1

	for reply.Success == false {

		//when reply term > currentTerm
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			DPrintf("current reply Term:%v\n", reply.Term)
			rf.BecomeFollower(reply.Term)
			convertFollower = true
			rf.mu.Unlock()
			break
		} else {

			if reply.Modify == true && nextIndexTmp > 1 &&
				rf.currentTerm == reply.Term && reply.Term == args.Term {

				//nextindex--
				nextIndexTmp--

				args.PrevLogIndex = nextIndexTmp - 1

				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term

				logs := make([]LogEntry, 1)
				logs[0] = rf.logs[nextIndexTmp]
				args.Entries = append(logs, args.Entries...)
				DPrintf("serverId:%v leaderCTerm:%v PrevLogIndex:%v reply.Term:%v success:%v Entries len:%v\n",
					serverId, rf.currentTerm, args.PrevLogIndex, reply.Term, reply.Success, len(args.Entries))

			} else if reply.Term > args.Term {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()

			reply = AppendEntriesReply{}

			//maybe blocked too
			peer.Call("Raft.AppendEntries", &args, &reply)
			time.Sleep(10 * time.Millisecond)

		}
	}

	//this stage is follower, return
	rf.mu.Lock()
	rf.leaderState.NextIndex[serverId] = nextIndexTmp

	if reply.Term > rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if convertFollower == false && reply.Modify == true && reply.Success == true {
		rf.leaderState.NextIndex[serverId] = max(rf.leaderState.NextIndex[serverId], reply.NextIndex)
		//rf.leaderState.NextIndex[serverId] += len(args.Entries)
		rf.leaderState.MatchIndex[serverId] = max(rf.leaderState.NextIndex[serverId]-1, 0)
		DPrintf("serverid:%v NextIndex:%v MatchIndex:%v", serverId, rf.leaderState.NextIndex[serverId], rf.leaderState.MatchIndex[serverId])
		waitMost.mu.Lock()
		if rf.leaderState.MatchIndex[serverId] >= commitedIndex {
			waitMost.number++
			DPrintf("SendAppendEntriesRPCToOneServer waitMost.number:%v\n", waitMost.number)
		}
		waitMost.mu.Unlock()
	} else {
		DPrintf("SendAppendEntries from :%v to :%v fail...\n", rf.me, serverId)
	}

	rf.mu.Unlock()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
