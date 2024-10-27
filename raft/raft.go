package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

const DebugMode = 1

type CommitEntry struct {
	// client command
	Command any
	// index of log entry
	Index int
	// term number
	Term int
}

type LogEntry struct {
	Command any
	Term    int
}

// states
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
	Dead // 論文上では存在しないが、RaftServiceが止まったらこのステートになる
)

func (s RaftState) String() string {
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
		panic("unreachable")
	}
}

// RaftService (ConsensusModule, CM)
type RaftService struct {
	mutex sync.Mutex
	// server id of the instance
	id int
	// peer server ids
	peerIds []int
	// Server struct to communicate other servers
	server *Server

	// persistent states
	currentTerm int
	voteFor     int
	log         []LogEntry

	// commit log channel
	commitChan chan<- CommitEntry
	// new commit ready channel to notify entries may be send to commitChan
	newCommitReadyChan chan struct{}

	// volatile states
	state              RaftState
	electionResetEvent time.Time
	commitIndex        int
	lastApplied        int

	// volatile states for leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

func NewRaftService(id int, peerIds []int, server *Server, ready <-chan interface{}, commitChan chan<- CommitEntry) *RaftService {
	rs := new(RaftService)
	rs.id = id
	rs.peerIds = peerIds
	rs.server = server
	rs.state = Follower
	rs.voteFor = -1
	rs.commitIndex = -1
	rs.lastApplied = -1
	rs.commitChan = commitChan
	rs.newCommitReadyChan = make(chan struct{}, 16)
	rs.nextIndex = make(map[int]int)
	rs.matchIndex = make(map[int]int)

	go func() {
		// ready は値を受信するまでブロックしている
		// 値を受信すると、選挙が開始される
		<-ready

		rs.mutex.Lock()
		rs.electionResetEvent = time.Now()
		rs.mutex.Unlock()
		rs.runElectionTimer()
	}()

	go rs.commitChanSender()
	return rs
}

func (rs *RaftService) Stop() {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	rs.state = Dead
	rs.dlog("becomes Dead")
	close(rs.newCommitReadyChan)
}

// Report the state of RaftService
func (rs *RaftService) Report() (id int, term int, isLeader bool) {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	return rs.id, rs.currentTerm, rs.state == Leader
}

// ランダムなタイムアウト時間を生成する
func (rs *RaftService) electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// 選挙タイマー
// 新しい選挙で新しい候補者を立てる時か新しい term に移る時にタイマーを作成する
// このタイマーはブロックするため、異なる goroutine で実行されることが期待される
func (rs *RaftService) runElectionTimer() {
	timeout := rs.electionTimeout()
	rs.mutex.Lock()
	startTerm := rs.currentTerm
	rs.mutex.Unlock()
	rs.dlog("election timer started (%v), term=%d", timeout, startTerm)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		// rs.dlog("election timer: startTerm=%d, term=%d, state=%s, electionResetEvent=%d", startTerm, rs.currentTerm, rs.state.String(), time.Since(rs.electionResetEvent))

		rs.mutex.Lock()
		if rs.state != Candidate && rs.state != Follower {
			rs.dlog("in election timer state=%s, bailing out", rs.state.String())
			rs.mutex.Unlock()
			return
		}
		if startTerm != rs.currentTerm {
			// term が変わっていたら何もせずに goroutine を終了する
			rs.dlog("in election time term changed from %d to %d, bailing out", startTerm, rs.currentTerm)
			rs.mutex.Unlock()
			return
		}
		// 次の場合には選挙を開始する
		// 	- タイムアウトまでにリーダーからの heartbeat が来ない
		// 	- タイムアウトまでに誰にも投票していない
		if elasped := time.Since(rs.electionResetEvent); elasped >= timeout {
			rs.startElection()
			rs.mutex.Unlock()
			return
		}
		rs.mutex.Unlock()
	}
}

func (rs *RaftService) startElection() {
	rs.state = Candidate
	rs.currentTerm += 1
	currentTerm := rs.currentTerm
	rs.electionResetEvent = time.Now()
	rs.voteFor = rs.id
	rs.dlog("startElection: becomes Candidate (currentTerm=%d); log=%v", currentTerm, rs.log)

	votesReceived := 1

	// send RequestVote RPCs to all other servers
	for _, peerId := range rs.peerIds {
		go func(peerId int) {
			rs.mutex.Lock()
			lastLogIndex, lastLogTerm := rs.lastLogIndexAndTerm()
			rs.mutex.Unlock()
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rs.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			rs.dlog("sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply
			if err := rs.server.Call(peerId, "RaftService.RequestVote", args, &reply); err == nil {
				rs.mutex.Lock()
				defer rs.mutex.Unlock()
				rs.dlog("received RequestVoteReply: %+v", reply)

				if rs.state != Candidate {
					rs.dlog("while waiting for reply, state = %v", rs.state)
					return
				}
				if reply.Term > currentTerm {
					rs.dlog("term out of date in RequestVote RPC")
					rs.becomeFollower(reply.Term)
					return
				} else if reply.Term == currentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived*2 > len(rs.peerIds)+1 {
							rs.dlog("wins election with %d votes", votesReceived)
							rs.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	// この選挙が失敗した場合に備えてもう一つのタイマーを動かす
	go rs.runElectionTimer()
}

func (rs *RaftService) lastLogIndexAndTerm() (int, int) {
	if len(rs.log) > 0 {
		lastIndex := len(rs.log) - 1
		return lastIndex, rs.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rs *RaftService) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	if rs.state == Dead {
		return nil
	}
	lastLogIndex, lastLogTerm := rs.lastLogIndexAndTerm()
	rs.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, rs.currentTerm, rs.voteFor, lastLogIndex, lastLogTerm)

	if args.Term > rs.currentTerm {
		rs.dlog("... term out of date in RequestVote")
		rs.becomeFollower(args.Term)
	}

	if args.Term == rs.currentTerm &&
		(rs.voteFor == -1 || rs.voteFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rs.voteFor = args.CandidateId
		rs.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rs.currentTerm
	rs.dlog("... RequestVote reply %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Success bool
	Term    int
}

func (rs *RaftService) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	if rs.state == Dead {
		return nil
	}
	rs.dlog("AppendEntries: %+v", args)

	// 1. term < currentTerm の場合は false と応答
	if rs.currentTerm < args.Term { // リーダーの方が term が進んでいる場合
		rs.dlog("... term out of date in AppendEntries")
		rs.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rs.currentTerm {
		// Candidate が存在する場合は Follower にする
		// リーダーが同時に2つ存在することはないので、Leader の場合も Candidate と同様
		if rs.state != Follower {
			rs.becomeFollower(args.Term)
		}
		rs.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(rs.log) && args.PrevLogTerm == rs.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// 挿入する場所を検索する
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0
			for {
				if logInsertIndex >= len(rs.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				// 3. 既存のエントリが新しいエントリと競合する場合 (同じインデックスだが異なるターム)、既存のエントリとそれに続くものをすべて削除
				if rs.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				rs.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				// 4. まだログにないエントリの場合は追加
				rs.log = append(rs.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				rs.dlog("... log is now: %v", rs.log)
			}
			if rs.commitIndex < args.LeaderCommit {
				// 5. leaderCommit > commitIndex の場合は commitIndex = min(leaderCommit, 最後の新しいエントリのインデックス) を設定
				rs.commitIndex = min(args.LeaderCommit, len(rs.log)-1)
				rs.dlog("... setting commitIndex := %d", rs.commitIndex)
				rs.newCommitReadyChan <- struct{}{}
			}
		}
	}

	reply.Term = rs.currentTerm
	rs.dlog("AppendEntries reply: %+v", *reply)

	return nil
}

func (rs *RaftService) startLeader() {
	rs.state = Leader
	rs.dlog("becomes Leader; term=%d, log=%v", rs.currentTerm, rs.log)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// send heartbeats
		for {
			rs.sendHeartbeats()
			<-ticker.C

			rs.mutex.Lock()
			if rs.state != Leader {
				rs.mutex.Unlock()
				return
			}
			rs.mutex.Unlock()
		}
	}()
}

func (rs *RaftService) sendHeartbeats() {
	rs.mutex.Lock()
	if rs.state != Leader {
		rs.mutex.Unlock()
		return
	}
	currentTerm := rs.currentTerm
	rs.mutex.Unlock()

	for _, peerId := range rs.peerIds {
		go func(peerId int) {
			rs.mutex.Lock()
			ni := rs.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = rs.log[prevLogIndex].Term
			}
			entries := rs.log[ni:]

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rs.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rs.commitIndex,
			}
			rs.mutex.Unlock()

			rs.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := rs.server.Call(peerId, "RaftService.AppendEntries", args, &reply); err == nil {
				rs.mutex.Lock()
				defer rs.mutex.Unlock()
				if reply.Term > currentTerm {
					rs.dlog("term out of date in heartbeat reply")
					rs.becomeFollower(reply.Term)
					return
				}

				if rs.state == Leader && currentTerm == reply.Term {
					if reply.Success {
						rs.nextIndex[peerId] = ni + len(entries)
						rs.matchIndex[peerId] = rs.nextIndex[peerId] - 1
						rs.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerId, rs.nextIndex, rs.matchIndex)

						commitIndex := rs.commitIndex
						for i := rs.commitIndex + 1; i < len(rs.log); i++ {
							if rs.log[i].Term == rs.currentTerm {
								matchCount := 1
								for _, peerId := range rs.peerIds {
									if rs.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(rs.peerIds)+1 {
									rs.commitIndex = i
								}
							}
						}
						if rs.commitIndex != commitIndex {
							rs.dlog("leader sets commitIndex := %d", rs.commitIndex)
							rs.newCommitReadyChan <- struct{}{}
						}
					} else {
						rs.nextIndex[peerId] = ni - 1
						rs.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
					}
				}
			}
		}(peerId)
	}
}

func (rs *RaftService) becomeFollower(term int) {
	rs.dlog("becomes Follower with term=%d; log=%v", term, rs.log)
	rs.state = Follower
	rs.currentTerm = term
	rs.voteFor = -1
	rs.electionResetEvent = time.Now()

	go rs.runElectionTimer()
}

func (rs *RaftService) dlog(format string, args ...interface{}) {
	if DebugMode > 0 {
		format = fmt.Sprintf("[%d] ", rs.id) + format
		log.Printf(format, args...)
	}
}

// if the command was send to the leader, it returns true otherwise false
func (rs *RaftService) Submit(command any) bool {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	rs.dlog("submit received by %v: %v", rs.state, command)
	if rs.state == Leader {
		rs.log = append(rs.log, LogEntry{command, rs.currentTerm})
		rs.dlog("... log=%v", rs.log)
		return true
	}
	return false
}

func (rs *RaftService) commitChanSender() {
	for range rs.newCommitReadyChan {
		// Find which entries we have to apply.
		rs.mutex.Lock()
		savedTerm := rs.currentTerm
		savedLastApplied := rs.lastApplied
		var entries []LogEntry
		if rs.commitIndex > rs.lastApplied {
			entries = rs.log[rs.lastApplied+1 : rs.commitIndex+1]
			rs.lastApplied = rs.commitIndex
		}
		rs.mutex.Unlock()
		rs.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			rs.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	rs.dlog("commitChanSender done")
}
