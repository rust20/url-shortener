package raft

import (
	"database/sql"
	"sync"
	"time"

	"errors"

	log "github.com/sirupsen/logrus"
)

const RETRY_COUNT = 3

var signal = struct{}{}

type Log struct {
	Term  uint64 `json:"term"`
	Idx   uint64 `json:"idx"`
	Op    int    `json:"op"`
	Key   string `json:"key"`
	Value string `json:"val"`
}

type NodeStatus int

const (
	NodeUnreachable NodeStatus = iota
	NodeActive
	NodeLeader
)

type Node struct {
	Address string
	Port    int
	Status  NodeStatus
}

type ModeState int

const (
	ModeLeader ModeState = iota
	ModeFollower
	ModeCandidate
)

type leaderData struct {
}

type State struct {
	mode ModeState
	// general persistent state
	// update before respond to rpcs
	// TODO: store in db
	CurrentTerm uint64
	VotedFor    *Node
	Logs        []Log
	LogsMu      sync.Mutex

	// general volatile state
	CommitIndex uint64
	LastApplied uint64

	// leader's volatile state
	// reset after elections

	NextIndex  []uint64
	MatchIndex []uint64

	ApplyLogCallback func(self *State)

	electionTimeout      time.Duration
	resetElectionTimeout chan<- any
	stopElectionTimer    chan<- any

	heartbeatInterval      time.Duration
	stopHeartbeatBroadcast chan<- any

	nodes    []Node
	selfNode Node
}

type NewStateArgs struct {
	ApplyLog          func(self *State)
	ElectionTimeout   time.Duration
	heartbeatInterval time.Duration
	SelfNode          Node
}

func NewState(db *sql.DB, args *NewStateArgs) *State {
	return &State{
		mode:        ModeFollower,
		CurrentTerm: 0,
		VotedFor:    nil,
		Logs:        []Log{},

		CommitIndex: 0,
		LastApplied: 0,

		NextIndex:  []uint64{},
		MatchIndex: []uint64{},

		ApplyLogCallback:     args.ApplyLog,
		electionTimeout:      0,
		resetElectionTimeout: make(chan<- any),
		stopElectionTimer:    make(chan<- any),

		heartbeatInterval:      0,
		stopHeartbeatBroadcast: make(chan<- any),

		nodes:    []Node{},
		selfNode: Node{},
	}
}

type AppendEntriesArgs struct {
	Term         uint64 `json:"term"`
	LeaderId     Node   `json:"leader_id"`
	PrevLogIndex uint64 `json:"prev_log_idx"`
	PrevLogTerm  uint64 `json:"prev_log_term"`
	Entries      []Log  `json:"entries"`
	LeaderCommit uint64 `json:"leader_commit"`
}

func (s *State) logLength() uint64 {
	return uint64(len(s.Logs))
}

func (s *State) startElectionTimer() (chan<- any, chan<- any) {
	reset := make(chan any)
	stop := make(chan any)

	go func() {
		for {
			select {
			case <-stop:
				break
			case <-reset:
				// TODO: do things when reset (probably nothing)
			case <-time.After(s.electionTimeout):
				s.ToCandidate()
			}
		}
	}()

	return reset, stop
}

func (s *State) startHeartbeatBroadcaster() chan<- any {
	stop := make(chan any)

	go func() {
		for {
			select {
			case <-stop:
				break
			case <-time.After(s.heartbeatInterval):
                s.BroadcastEntries(true)
			}
		}
	}()

	return stop
}

var ErrAppendEntriesFail = errors.New("append failed")

// invoked by leader
func (s *State) AppendEntries(args AppendEntriesArgs) (uint64, bool) {
	if args.Term < s.CurrentTerm {
		if s.mode == ModeLeader {
			s.ToFollower()
		}
	}

	s.resetElectionTimeout <- signal
	s.LogsMu.Lock()

	if s.logLength() < args.PrevLogIndex || s.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		s.LogsMu.Unlock()
		return s.CurrentTerm, false
	}

	// TODO: maybe checking from the latest entries would be faster?
	// TODO: binary search could world aswell for large number of entries
	for _, entry := range args.Entries {
		if entry.Term != s.Logs[entry.Idx].Term {
			s.Logs = s.Logs[:entry.Idx]
			break
		}
	}

	s.Logs = append(s.Logs, args.Entries...)
	s.LogsMu.Unlock()

	if args.LeaderCommit > s.CommitIndex {
		s.CommitIndex = min(args.LeaderCommit, s.CommitIndex)
	}
	if s.CommitIndex > s.LastApplied {
		s.ApplyLogCallback(s)
		s.LastApplied = s.CommitIndex
	}

	return s.CurrentTerm, true
}

type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateId  Node   `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_idx"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// invoked by candiates
func (s *State) RequestVote(args RequestVoteArgs) (uint64, bool) {
	if args.Term < s.CurrentTerm {
		return s.CurrentTerm, false
	}

	// TODO: if is leader/candidate with < term, immediately change to follower

	s.resetElectionTimeout <- signal
	s.LogsMu.Lock()
	defer s.LogsMu.Unlock()

	grantVote := (s.VotedFor == nil || *s.VotedFor == args.CandidateId) &&
		(args.Term >= s.CurrentTerm && args.LastLogIndex >= s.logLength())

	return s.CurrentTerm, grantVote
}

func (s *State) ToFollower() {
	s.mode = ModeFollower

	select {
	case s.stopHeartbeatBroadcast <- signal:
		// message successfully sent
	default:
		// message dropped, probably its closed because no thing is running
	}

	resetChan, stopChan := s.startElectionTimer()
	s.resetElectionTimeout = resetChan
	s.stopElectionTimer = stopChan
}

func (s *State) ToCandidate() {
	s.mode = ModeCandidate
	s.CurrentTerm += 1
	s.VotedFor = &s.selfNode

	s.resetElectionTimeout <- signal

	// voteChannel := make(chan int)
	// gotNewLeader := make(chan any)
	acquiredVotes := 0

	s.LogsMu.Lock()
	defer s.LogsMu.Unlock()

	for _, node := range s.nodes {
		lastLogterm := uint64(0)

		if s.logLength() > 0 {
			lastLogterm = s.Logs[s.logLength()].Term
		}

		payload := &RequestVoteArgs{
			Term:         s.CurrentTerm,
			CandidateId:  s.selfNode,
			LastLogIndex: s.logLength(),
			LastLogTerm:  lastLogterm,
		}

		// TODO: pass to goroutine
		// var wg sync.WaitGroup
		//
		// go func() {
		// 	wg.Add(1)
		// 	defer wg.Done()
		//
		// 	for i := 0; ; i += 1 {
		// 		resp, err := s.SendRequestVote(node, payload)
		// 		if err == nil {
		// 			if resp.Term > s.CurrentTerm {
		// 				gotNewLeader <- signal
		// 				s.ToFollower()
		// 				break
		// 			}
		// 			if resp.VoteGranted {
		// 				voteChannel <- 1
		// 			} else {
		// 				voteChannel <- 0
		// 			}
		// 			break
		// 		}
		// 		time.Sleep(time.Duration(i*100) * time.Millisecond)
		// 	}
		// }()
		resp, err := s.SendRequestVote(node, payload)
		if err != nil {
			log.Errorf("failed to send request vote %v", err)
		}
		if resp.Term > s.CurrentTerm {
			// TODO: when impl for concurrency, 
            //       move this after the for loop
			s.ToFollower()
            return
		}
		if resp.VoteGranted {
			acquiredVotes += 1
			// 	acquiredVotes <- 1
			// } else {
			// 	acquiredVotes <- 0
		}
	}

	// for i := 0; i < len(s.nodes); i += 1 {
	// 	acquiredVotes += <-voteChannel
	// }

	if hasMajorityVote(acquiredVotes, len(s.nodes)) {
		s.ToLeader()
	}
}

func hasMajorityVote(vote int, total int) bool {
	return vote > (total - vote)
}

func (s *State) ToLeader() {
	s.mode = ModeLeader
	select {
	case s.stopElectionTimer <- signal:
		// message successfully sent
	default:
		// message dropped, probably its closed
	}
	close(s.stopElectionTimer)
	close(s.resetElectionTimeout)

	stopChan := s.startHeartbeatBroadcaster()
	s.stopHeartbeatBroadcast = stopChan

	s.NextIndex = make([]uint64, len(s.nodes))
	s.MatchIndex = make([]uint64, len(s.nodes))

	s.LogsMu.Lock()
	defer s.LogsMu.Unlock()

	lastLogIndex := s.logLength()
	for i := 0; i < len(s.nodes); i += 1 {
		s.NextIndex[i] = lastLogIndex
	}
}

// GetLogs will get list of logs from memory. Not thread safe.
func (s *State) GetLogs(start int, count int) ([]Log, error) {
    l := s.Logs[start:start+count]
    return l, nil
}
