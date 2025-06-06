package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft peer.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log Entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new Entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same peer.
//

import (
	"6.5840/labgob"
	"bytes"
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
// tester) on the same peer, via the applyCh passed to Make(). set
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
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int                 //节点保存的最新的Term
	state       int                 //节点当前的状态
	votedFor    int                 //投给的节点
	votes       int                 //选票数
	timer       *time.Timer         //计时器
	voteTimeout time.Duration
	log         []Entry //日志
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyChan   chan ApplyMsg

	snapshot         []byte //快照
	lastIncludeIndex int    //快照里最后一个日志的index
	lastIncludeTerm  int    //快照里最后一个日志的term
}

// return currentTerm and whether this peer
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)

	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// 没有持久化数据，是一个新的节点
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm_ int
	var votedFor_ int
	var log_ []Entry
	var lastIndex int
	var lastTerm int
	if d.Decode(&currentTerm_) != nil || d.Decode(&votedFor_) != nil ||
		d.Decode(&log_) != nil || d.Decode(&lastIndex) != nil || d.Decode(&lastTerm) != nil {
		DPrintf("[readPersist] errors")
	} else {
		rf.currentTerm = currentTerm_
		rf.votedFor = votedFor_
		rf.log = log_
		rf.lastIncludeTerm = lastTerm
		rf.lastIncludeIndex = lastIndex
	}
}

func (rf *Raft) readSnapshot(snapshot []byte) {
	rf.snapshot = snapshot
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 无法截取
	if index > rf.commitIndex || index <= rf.lastIncludeIndex {
		return
	}

	// 截取日志
	logIndex := rf.toLogIndex(index)
	rf.lastIncludeTerm = rf.log[logIndex].Term
	rf.log = append([]Entry{{rf.log[logIndex].Term, nil}}, rf.log[logIndex+1:]...)
	rf.lastIncludeIndex = index

	// 加载快照
	rf.snapshot = snapshot

	// 已经存入快照了，直接更新lastApplied
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persist()
}

// Start the service using Raft (e.g. a k/v peer) wants to start
// agreement on the next command to be appended to Raft's log. if this
// peer isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this peer believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
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
	rf.persist()
	rf.nextIndex[rf.me] = rf.toGlobalIndex(len(rf.log))
	rf.matchIndex[rf.me] = rf.toGlobalIndex(len(rf.log) - 1)
	return rf.matchIndex[rf.me], rf.currentTerm, true
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int //当前的term
	CandidateId  int //当前request来自节点的Id
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  //candtidate的term
	VoteGranted bool //是否收到票
}

// AppendEntriesArgs Leader给follower发送复制日志和用于心跳
type AppendEntriesArgs struct {
	Term        int
	LeaderId    int
	PreLogIndex int
	PreLogTerm  int
	Entries     []Entry
	LeadCommit  int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term in the conflicting entry (if any)
	XIndex  int // index of first entry with that term (if any)
	XLen    int // log length
}

type InstallSnapshotArgs struct {
	Term      int
	LeaderId  int
	LastIndex int
	LastTerm  int
	Data      []byte
}
type InstallSnapshotReply struct {
	Term int
}

// 加载快照
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		reply.Term = -1
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader已经过时了
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	//写入snapshot
	if args.LastIndex > rf.lastIncludeIndex {
		logIndex := rf.toLogIndex(args.LastIndex)

		if logIndex < len(rf.log) && rf.log[logIndex].Term == args.LastTerm {
			// 日志更长，包含该快照的部分了
			rf.log = append([]Entry{{rf.log[logIndex].Term, nil}}, rf.log[logIndex+1:]...)
		} else {
			// 日志没有出现快照中的部分
			rf.log = make([]Entry, 0)
			rf.log = append(rf.log, Entry{
				Term:    args.LastTerm,
				Command: nil,
			})
		}
		rf.snapshot = args.Data
		rf.lastIncludeIndex = args.LastIndex
		rf.lastIncludeTerm = args.LastTerm
		if rf.commitIndex < rf.lastIncludeIndex {
			rf.commitIndex = rf.lastIncludeIndex
		}
		if rf.lastApplied < rf.lastIncludeIndex {
			rf.lastApplied = rf.lastIncludeIndex
		}
		// 应用到状态机上
		msg := ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  0,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastTerm,
			SnapshotIndex: args.LastIndex,
		}
		rf.applyChan <- msg
	}

	rf.changeToFollower(args.Term, -1)
	reply.Term = rf.currentTerm

	// 持久化
	rf.persist()
}

// 接收心跳和日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	// 更新节点信息和超时时间
	rf.changeToFollower(args.Term, -1)
	reply.Term = rf.currentTerm

	if args.PreLogIndex < rf.lastIncludeIndex {
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.toGlobalIndex(len(rf.log))
		return
	}

	// index不匹配,该节点日志太短了
	if args.PreLogIndex >= rf.toGlobalIndex(len(rf.log)) {
		reply.XIndex = -1
		reply.XTerm = -1
		reply.XLen = rf.toGlobalIndex(len(rf.log))
		return
	}

	// term不匹配，找到该term的第一个日志位置
	if args.PreLogTerm != rf.log[rf.toLogIndex(args.PreLogIndex)].Term {
		reply.XTerm = rf.log[rf.toLogIndex(args.PreLogIndex)].Term
		reply.XIndex = args.PreLogIndex
		for rf.toLogIndex(reply.XIndex) > 0 && rf.log[rf.toLogIndex(reply.XIndex-1)].Term == reply.XTerm {
			reply.XIndex--
		}
		reply.XLen = rf.toGlobalIndex(len(rf.log))
		return
	}

	// 添加新日志
	rf.log = append(rf.log[:rf.toLogIndex(args.PreLogIndex)+1], args.Entries...)
	rf.persist()
	reply.Success = true

	// 更新commitId
	if args.LeadCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeadCommit, rf.toGlobalIndex(len(rf.log)-1))
	}

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

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
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < rf.toGlobalIndex(len(rf.log)-1)) {
		reply.Term = rf.currentTerm
		return
	}

	//candidate的term大，转换成追随者
	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term, -1)
	}

	//未投票,可以投票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.resetTimer()
	}

}

func (rf *Raft) heartBeatHandler(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	if rf.killed() {
		return false
	}

	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 当前的term发生了变化
	if args.Term != rf.currentTerm {
		return false
	}

	return reply.Success
}

func (rf *Raft) requestVoteHandler(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[peer].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 当前的term发生了变化
	if args.Term != rf.currentTerm {
		return false
	}

	return reply.VoteGranted
}

func (rf *Raft) installSnapshotHandler(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 当前的term发生了变化
	if args.Term != rf.currentTerm || reply.Term > rf.currentTerm {
		return false
	}

	return true
}

func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			// TODO
			// 该follower的日志太落后了，leader已经没有了,直接传快照
			if rf.nextIndex[peer] <= rf.lastIncludeIndex {
				args := &InstallSnapshotArgs{
					Term:      rf.currentTerm,
					LeaderId:  rf.me,
					LastIndex: rf.lastIncludeIndex,
					LastTerm:  rf.lastIncludeTerm,
					Data:      rf.snapshot,
				}
				go rf.collectSnapshot(peer, args)
				continue
			}
			//
			logIndex := rf.toLogIndex(rf.nextIndex[peer])
			//var preLogTerm int
			//if logIndex == 0 {
			//	preLogTerm = rf.lastIncludeTerm
			//} else {
			//	preLogTerm = rf.log[logIndex-1].Term
			//}
			entries := rf.log[logIndex:]

			args := &AppendEntriesArgs{
				Term:        rf.currentTerm,
				LeaderId:    rf.me,
				PreLogIndex: rf.nextIndex[peer] - 1,
				//PreLogTerm:  preLogTerm,
				PreLogTerm: rf.log[logIndex-1].Term,
				Entries:    entries,
				LeadCommit: rf.commitIndex,
			}
			go rf.collectReplicates(peer, args)
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) collectSnapshot(peer int, args *InstallSnapshotArgs) {
	//TODO
	reply := &InstallSnapshotReply{}
	if ok := rf.installSnapshotHandler(peer, args, reply); !ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.changeToFollower(reply.Term, -1)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.matchIndex[peer] = args.LastIndex
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1

}

func (rf *Raft) collectReplicates(peer int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	// TODO
	if ok := rf.heartBeatHandler(peer, args, reply); !ok {
		rf.mu.Lock()
		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		//leader发现当前节点的term已经比自己的大，转变为follower
		if reply.Term > rf.currentTerm {
			rf.changeToFollower(reply.Term, -1)
			rf.mu.Unlock()
			return
		}
		// 如果该节点日志列表太短了，直接把nextIndex变成该节点日志列表长度
		if reply.XTerm == -1 {
			rf.nextIndex[peer] = reply.XLen
		} else { // 日志列表一样长，但是term不一样，需要考虑把这个term的日志全部更新
			f := false
			// leader找自己日志列表中这个term的第一个位置,从这个位置开始更新
			for i := 0; i < len(rf.log); i++ {
				if rf.log[i].Term == reply.XTerm {
					f = true
					rf.nextIndex[peer] = rf.toGlobalIndex(i)
					break
				}
			}
			if !f { // leader根本没有这个term的日志，直接从XIndex处开始更新
				rf.nextIndex[peer] = reply.XIndex
			}
		}
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.matchIndex[peer] = args.PreLogIndex + len(args.Entries)
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1

	// 判断可以更新commitId吗
	N := len(rf.log) - 1
	for N > rf.toLogIndex(rf.commitIndex) {
		cnt := 1
		for i, idx := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			if rf.toLogIndex(idx) >= N && rf.log[N].Term == rf.currentTerm {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = rf.toGlobalIndex(N)
			rf.persist()
			break
		}
		N--
	}
}

func (rf *Raft) collectVote(peer int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}

	if ok := rf.requestVoteHandler(peer, args, reply); !ok {
		// 该节点没有投票
		rf.mu.Lock()

		// 该节点的term更大，变成follower
		if reply.Term > rf.currentTerm {
			// 转换为follower
			rf.changeToFollower(reply.Term, -1)
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

	// 获得半数以上的节点的投票，变成LEADER
	if rf.votes > len(rf.peers)/2 && rf.state == CANDIDATE {
		rf.changeToLeader()
	}
}

func (rf *Raft) startCandidate() {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.toGlobalIndex(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	// 向每个节点发送投票RPC
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.collectVote(idx, args)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.timer.C:
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.changeToCandidate()
				go rf.startCandidate()
			}
			rf.resetTimer()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) changeToLeader() {
	rf.state = LEADER
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = rf.toGlobalIndex(len(rf.log))
		rf.matchIndex[idx] = rf.lastIncludeIndex
	}
	go rf.sendHeartBeats()
}

func (rf *Raft) changeToCandidate() {
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votes = 1
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) changeToFollower(term int, voteFor int) {
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votes = 0
	rf.votedFor = voteFor
	rf.persist()
	rf.resetTimer()
}

func (rf *Raft) toGlobalIndex(lidx int) int {
	return lidx + rf.lastIncludeIndex
}

func (rf *Raft) toLogIndex(gidx int) int {
	return gidx - rf.lastIncludeIndex
}

func (rf *Raft) resetTimer() {
	rf.voteTimeout = time.Duration(rand.Int63()%300 + 150)
	if rf.timer == nil {
		rf.timer = time.NewTimer(rf.voteTimeout * time.Millisecond)
		return
	}
	rf.timer.Reset(rf.voteTimeout * time.Millisecond)
}

// CommitHandler 提交日志到状态机，每50ms提交一次
func (rf *Raft) commitHandler() {
	// TODO
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex < rf.lastIncludeIndex {
			rf.commitIndex = rf.lastIncludeIndex
			rf.lastApplied = rf.lastIncludeIndex
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if rf.lastApplied < rf.lastIncludeIndex {
			rf.lastApplied = rf.lastIncludeIndex
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		msgBuf := make([]*ApplyMsg, 0)

		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.toGlobalIndex(len(rf.log)-1) {
			rf.lastApplied++
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.toLogIndex(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			}
			msgBuf = append(msgBuf, msg)
		}
		rf.mu.Unlock()

		for _, msg := range msgBuf {
			rf.applyChan <- *msg
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft peer. the ports
// of all the Raft peers (including this one) are in peers[]. this
// peer's port is peers[me]. all the peers' peers[] arrays
// have the same order. persister is a place for this peer to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		mu:               sync.Mutex{},
		peers:            peers,
		persister:        persister,
		me:               me,
		dead:             0,
		currentTerm:      0,
		state:            FOLLOWER,
		votedFor:         -1,
		votes:            0,
		log:              make([]Entry, 0),
		commitIndex:      0,
		lastApplied:      0,
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		applyChan:        applyCh,
		snapshot:         nil,
		lastIncludeIndex: 0,
		lastIncludeTerm:  0,
	}

	// 填入0处的占位日志
	rf.log = append(rf.log, Entry{Term: 0, Command: nil})

	// 宕机恢复后，读取持久化的数据
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// 更新nextIndex
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = rf.toGlobalIndex(len(rf.log))
	}

	//设置超时时间
	rf.resetTimer()

	//开始状态机
	go rf.ticker()
	//开始提交协程
	go rf.commitHandler()

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
