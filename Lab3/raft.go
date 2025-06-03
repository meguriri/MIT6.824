package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log Entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new Entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log Entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	//3A
	currentTerm int         //节点保存的最新的Term
	state       int         //节点当前的状态
	votedFor    int         //投给的节点
	votes       int         //选票数
	timer       *time.Timer //计时器
	voteTimeout time.Duration
	//3B
	log         []Entry //日志
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyChan   chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool
	// Your code here (3A)
	term = rf.currentTerm
	isLeader = rf.state == LEADER

	return term, isLeader
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

// Snapshot the service says it has created a snapshot that has
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
	//3A
	Term        int //当前的term
	CandidateId int //当前request来自节点的Id
	//3B
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A)
	Term        int  //candtidate的term
	VoteGranted bool //是否收到票
}

// Leader给follower发送复制日志和用于心跳
type AppendEntriesArgs struct {
	//3A
	Term     int //leader's term
	LeaderId int
	//3B
	PreLogIndex int
	PreLogTerm  int
	Entries     []Entry
	LeadCommit  int
}
type AppendEntriesReply struct {
	//3A
	Term    int
	Success bool
}

// 接收心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term >= rf.currentTerm {
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm

		rf.timer.Reset(rf.voteTimeout * time.Millisecond)
		rf.state = FOLLOWER

		// index不匹配
		if args.PreLogIndex >= len(rf.log) {
			reply.Success = false
			return
		}

		// term不匹配，需要删除不匹配的日志
		if args.PreLogTerm != rf.log[args.PreLogIndex].Term {
			reply.Success = false
			return
		}

		// 删除不匹配的日志
		rf.log = rf.log[:args.PreLogIndex+1]

		// 添加新日志
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true

		// 更新commitId
		if args.LeadCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeadCommit, len(rf.log)-1)
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	//candidate的term小，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	//candidate的日志不够新
	if args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1) {
		reply.Term = rf.currentTerm
		return
	}

	//candidate的term大，转换成追随者
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	//未投票,可以投票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.timer.Reset(rf.voteTimeout * time.Millisecond)
	}

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
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	//当前的term发生了变化
	if args.Term != rf.currentTerm {
		return false
	}

	// 已经是过时的term了
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	return reply.VoteGranted
}

func (rf *Raft) SendHeartBeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		for idx := range rf.peers {
			if idx == rf.me {
				continue
			}
			entries := make([]Entry, 0)
			for i := rf.nextIndex[idx]; i < len(rf.log); i++ {
				entries = append(entries, rf.log[i])
			}
			args := &AppendEntriesArgs{
				Term:        rf.currentTerm,
				LeaderId:    rf.me,
				PreLogIndex: rf.nextIndex[idx] - 1,
				PreLogTerm:  rf.log[rf.nextIndex[idx]-1].Term,
				Entries:     entries,
				LeadCommit:  rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			go rf.sendHeartBeat(idx, args, reply)
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		return false
	}

	//leader发现当前的term已经比自己的大，转变为follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.voteTimeout = time.Duration(rand.Int63()%300 + 150)
		rf.timer.Reset(rf.voteTimeout * time.Millisecond)
		return reply.Success
	}

	if reply.Success {
		rf.matchIndex[server] = args.PreLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// 判断可以commit吗
		N := len(rf.log) - 1
		for N > rf.commitIndex {
			cnt := 1
			for peer, idx := range rf.matchIndex {
				if peer == rf.me {
					continue
				}
				if idx >= N && rf.log[N].Term == rf.currentTerm {
					cnt++
				}

			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
			N--
		}

		return reply.Success
	}

	if rf.currentTerm == reply.Term && rf.state == LEADER {
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		} else {
			rf.nextIndex[server] = 1
		}
	}

	return reply.Success
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
	// Your code here (3B).
	if rf.killed() {
		return -1, -1, false
	}
	if rf.state != LEADER {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ent := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, ent)
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	return len(rf.log) - 1, rf.currentTerm, true

}

func (rf *Raft) CollectVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}

	if ok := rf.sendRequestVote(server, args, reply); !ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			// 切换为follower
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.voteTimeout = time.Duration(rand.Int63()%300 + 150)
			rf.timer.Reset(rf.voteTimeout * time.Millisecond)
		}
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 已经获得足够的选票了，无需再更新
	if rf.votes > len(rf.peers)/2 {
		return
	}

	rf.votes++

	if rf.votes > len(rf.peers)/2 && rf.state == CANDIDATE {
		rf.state = LEADER
		for idx := range rf.nextIndex {
			rf.nextIndex[idx] = len(rf.log)
			rf.matchIndex[idx] = 0
		}
		go rf.SendHeartBeats()
	}
}

func (rf *Raft) TimeOut() {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votes = 1
	rf.votedFor = rf.me
	rf.voteTimeout = time.Duration(rand.Int63()%300 + 150)
	rf.timer.Reset(rf.voteTimeout * time.Millisecond)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.CollectVote(idx, args)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.timer.C:
			rf.mu.Lock()
			if rf.state != LEADER {
				go rf.TimeOut()
			}
			rf.voteTimeout = time.Duration(rand.Int63()%300 + 150)
			rf.timer.Reset(rf.voteTimeout * time.Millisecond)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) CommitHandler() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < len(rf.log)-1 {
			rf.lastApplied++
			ap := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyChan <- ap
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		currentTerm: 0,
		state:       FOLLOWER,
		votedFor:    -1,
		votes:       0,
		log:         make([]Entry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyChan:   applyCh,
	}
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = 1
	}
	rf.log = append(rf.log, Entry{Term: 0, Command: nil})
	rf.voteTimeout = time.Duration(rand.Int63()%200 + 150)
	rf.timer = time.NewTimer(rf.voteTimeout * time.Millisecond)

	//3A
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	//开始状态机
	go rf.ticker()
	go rf.CommitHandler()

	return rf
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timer.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}
