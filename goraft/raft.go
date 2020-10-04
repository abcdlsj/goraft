package goraft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const IsDebug = 1

type LogEntry struct {
	Command interface{}
	Term    int
}

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		return "unreachable"
	}
}

type ConsensusModule struct {
	mu      sync.Mutex
	id      int // 机器 id
	peerIds []int

	server *Server

	// 持久化状态
	currentTerm int        // server 存储的最新任期
	votedFor    int        // 投票
	log         []LogEntry // log 条目

	// 可变状态
	commitIndex int     // 将被提交的日志记录的索引（初值为 0 且单调递增）
	lastApplied int     // 已经被提交到状态机的最后一个日志的索引（初值为 0 且单调递增）
	state       CMState // 机器状态

	// Leader 的可变状态，每次选举后重新初始化
	nextIndex          map[int]int // 每台机器在数组占据一个元素，元素的值为下条发送到该机器的日志索引 (初始值为 leader 最新一条日志的索引 +1)
	matchIndex         map[int]int // 每台机器在数组中占据一个元素，元素的记录将要复制给该机器日志的索引的
	electionResetEvent time.Time
}

// debuglog，当 IsDebug 大于 0，打印 log 用于 debug
func (cm *ConsensusModule) debuglog(format string, args ...interface{}) {
	//
	if IsDebug > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.debuglog("Become Dead.")
}

type RequestVoteArgs struct {
	Term         int // 候选者的任期
	CandidateID  int // 候选者编号
	LastLogIndex int // 候选者最后一条日志记录的索引
	LastLogItem  int // 候选者最后一条日志记录的索引的任期
}

type RequestVoteReply struct {
	Term        int  // 当前任期，候选者用来更新自己
	VoteGranted bool // 如果候选者当选则为 True
}

//
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	cm.debuglog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	if args.Term > cm.currentTerm {
		cm.debuglog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}
	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateID) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateID
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.debuglog("... RequestVote reply: %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term         int        // leader 任期
	LeaderId     int        // 用来 follower 重定向到 leader
	PrevLogIndex int        // 前继日志记录的索引
	prevLogItem  int        // 前继日志的任期
	Entries      []LogEntry // 存储日志记录
	LeaderCommit int        // leader 的 commitIndex
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，leader 用来更新自己
	success bool // 如果 follower 包含索引为 prevLogIndex 和任期为 prevLogItem 的日志
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	cm.debuglog("AppendEntries: +v", args)

	if args.Term > cm.currentTerm {
		cm.debuglog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.success = true
	}
	reply.Term = cm.currentTerm
	cm.debuglog("AppendEntries reply: %+v", reply)
	return nil
}

// 选举超时
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// If RAFT_FORCE_MORE_REELECTION is set, stress-test by deliberately
	// generating a hard-coded number very often. This will create collisions
	// between different servers and force more re-elections.
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// 选举定时器
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.debuglog("election timer started (%v), term = %d", timeoutDuration, termStarted)

	// 循环停止条件：
	// - 不再需要选举定时器了
	// - 当前 CM 成为候选人
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		// cm.state 不是 candidate 或者 follower 就退出
		if cm.state != Candidate && cm.state != Follower {
			cm.debuglog("int election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		//
		if termStarted != cm.currentTerm {
			cm.debuglog("in election timer term changed from %d to %d. bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// 如果没有收到 Leader 来信和没有投票给其它机器就开始选举
		// 使用自定义的选举重置时间，如果距离上次选举重置事件超过自定义超时，就开始选举
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

// 成为候选人：
// - 选择状态为 candidate，然后 currentTerm + 1
// - 向所有机器发送 RV RPCs，告诉其它机器需要向我们投票
// - 接受这些回应，并且检查是否获得大多数投票成为 Leader
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm++
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.debuglog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	var votesReceived int32 = 1

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateID: cm.id,
			}
			var reply RequestVoteReply

			cm.debuglog("sending RequestVote to %d: %+v", peerId, args)

			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.debuglog("received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.debuglog("while waiting for reply, state = %v", cm.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					cm.debuglog("term out of date in RequestVoteRepl")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))

						if votes*2 > len(cm.peerIds)+1 {
							cm.debuglog("win election with %d votes", votes)
							cm.startLeader()
							return
						}
					}
				}
			}

		}(peerId)
	}

	go cm.runElectionTimer()
}

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1

	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	return cm
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Lock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.debuglog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.debuglog("becomes leader; term=%d, log=%v", cm.currentTerm, cm.log)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			cm.leaderSendHeartbeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}

		go func(peerId int) {
			cm.debuglog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply

			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.debuglog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}
