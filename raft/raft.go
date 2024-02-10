package raft

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"errors"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"gitlab.db.in.tum.de/rust20/url-shortener/config"
)

const RETRY_COUNT = 3

var ErrAppendEntriesFail = errors.New("append failed")

var signal = struct{}{}

type Log struct {
	Id    int    `json:"-" db:"log_id"`
	Term  int    `json:"term" db:"log_term"`
	Idx   int    `json:"idx" db:"log_idx"`
	Op    int    `json:"op" db:"op"`
	Key   string `json:"key" db:"strkey"`
	Value string `json:"val" db:"strval"`
}

type NodeStatus int

const (
	NodeUnreachable NodeStatus = iota
	NodeActive
	NodeLeader
)

type Node struct {
	// ID     int
	Addr   string `db:"address"`
	Status NodeStatus
}

type ModeState int

const (
	ModeLeader ModeState = iota
	ModeFollower
	ModeCandidate
)

type State struct {
	mode   ModeState
	ModeMu sync.Mutex

	// general persistent state
	// update before respond to rpcs
	Term     int
	TermMu   sync.Mutex // TODO: use mutex
	VotedFor *Node
	VoteMu   sync.Mutex // TODO: use mutex
	LogsMu   sync.Mutex

	// general volatile state
	CommitIndex int
	CommitMu    sync.Mutex // TODO: use mutex
	LastApplied int
	AppliedMu   sync.Mutex // TODO: use mutex

	// leader's volatile state
	// reset after elections

	NextIndex  []int
	NIndexMu   sync.Mutex // TODO: use mutex
	MatchIndex []int
	MIndexMu   sync.Mutex // TODO: use mutex

	ApplyLogCallback func(self *State)

	electionTimeout      time.Duration
	resetElectionTimeout chan<- any
	stopElectionTimer    chan<- any

	heartbeatInterval      time.Duration
	stopHeartbeatBroadcast chan<- any

	nodes    []Node
	nodesMu  sync.Mutex // TODO: use mutex
	selfNode int

	db      *database
	prevIdx int
}

type NewStateArgs struct {
	ApplyLog          func(self *State)
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	SelfNode          int
	Config            config.Config
}

var clog *log.Entry

func New(db *sqlx.DB, args *NewStateArgs) *State {
    clog = log.WithField("node", args.SelfNode)

	timeoutOffset := time.Duration(rand.Intn(args.Config.ElectionTimeoutRange)) * time.Millisecond

	clog.Info("timout offset -> ", timeoutOffset)
	time.Sleep(2 * time.Second)

	electionTimeout := args.Config.ElectionTimeoutBase + timeoutOffset

	conn := &database{db}

	nodes, err := conn.GetNodes()
	if err != nil {
		clog.Panicf("raft: failed to get nodes: %v", err)
	}

	for _, node := range args.Config.Nodes {
		nodes = append(nodes, Node{
			Addr: node.Addr,
			// Status:  ,
		})
	}

	// TODO: update term
	term, err := conn.GetValInt("term")
	if err == sql.ErrNoRows {
		term = 0
	} else if err != nil {
		clog.Panicf("raft: failed to get term from db: %v", err)
	}

	// TODO: update voted for
	var votedFor *Node = nil
	votedForAddr, _ := conn.GetValString("votedFor")

	if err == sql.ErrNoRows {
		votedForAddr = ""
	} else if err != nil {
		clog.Panicf("raft: failed to get votedfor from db: %v", err)
	}

	if votedForAddr != "" {
		votedFor = &Node{Addr: votedForAddr}
	}

	return &State{
		mode:     ModeFollower,
		Term:     term,
		VotedFor: votedFor,
		LogsMu:   sync.Mutex{},

		CommitIndex: -1,
		LastApplied: -1,
		NextIndex:   []int{},
		MatchIndex:  []int{},

		ApplyLogCallback:       args.ApplyLog,
		electionTimeout:        electionTimeout,
		resetElectionTimeout:   make(chan<- any),
		stopElectionTimer:      make(chan<- any),
		heartbeatInterval:      args.HeartbeatInterval,
		stopHeartbeatBroadcast: make(chan<- any),
		nodes:                  nodes,
		selfNode:               args.SelfNode,

		db: conn,
	}
}

func (s *State) Run() {
	s.ToFollower()
	s.StartServer()
}

type AppendEntriesArgs struct {
	Term         int   `json:"term"`
	LeaderId     Node  `json:"leader_id"`
	PrevLogIndex int   `json:"prev_log_idx"`
	PrevLogTerm  int   `json:"prev_log_term"`
	Entries      []Log `json:"entries"`
	LeaderCommit int   `json:"leader_commit"`
}

// Not thread safe.
func (s *State) logLength() (int, error) {
	return s.db.LogsLength()
}

// Is thread safe.
func (s *State) startElectionTimer() (chan<- any, chan<- any) {
	reset := make(chan any)
	stop := make(chan any)

	go func() {
		for {
			select {
			case <-stop:
				break
			case <-reset:
				// do nothing
			case <-time.After(s.electionTimeout):
				go s.ToCandidate()
			}
		}
	}()

	return reset, stop
}

// Is thread safe.
func (s *State) startHeartbeatBroadcaster() chan<- any {
    clog.Info("BROADCASTING YOOOO")
	stop := make(chan any)

	go func() {
		for {
			select {
			case <-stop:
				break
			case <-time.After(s.heartbeatInterval):
				go s.BroadcastEntries(true)
			}
		}
	}()

	return stop
}

// invoked by leader
// Is thread safe.
func (s *State) AppendEntries(args AppendEntriesArgs) (int, bool) {
	if args.Term > s.Term {
		if s.mode == ModeLeader {
			s.Term = args.Term
			s.ToFollower()
		}
	}

	s.resetElectionTimeout <- signal
	s.LogsMu.Lock()

	logLength, err := s.logLength()
	if err != nil {
		s.LogsMu.Unlock()
		clog.Panicf("raft: failed to get log length")
	}

	if logLength < args.PrevLogIndex {
		s.LogsMu.Unlock()
		return s.Term, false
	}

	prevLog, err := s.db.GetLogByIdx(args.PrevLogIndex)
	if err != nil {
		s.LogsMu.Unlock()
		clog.Panicf("raft: failed to get log length: %s", err.Error())
	}

	if prevLog.Term != args.PrevLogTerm {
		s.LogsMu.Unlock()
		return s.Term, false
	}

	if len(args.Entries) == 0 {
		// TODO: maybe just return??
	}

	logs, err := s.db.ListLogs(args.PrevLogIndex+1, len(args.Entries))

	deleteFrom := 0

	for i, entry := range args.Entries {
		if entry.Term != logs[entry.Idx-args.PrevLogIndex].Term {
			deleteFrom = i
			break
		}
	}

	err = s.db.DeleteLogs(args.Entries[deleteFrom].Idx)
	if err != nil {
		s.LogsMu.Unlock()
		clog.Panicf("raft: failed to delete log: %s", err.Error())
	}

	err = s.db.InsertLogs(args.Entries[deleteFrom:])
	if err != nil {
		s.LogsMu.Unlock()
		clog.Panicf("raft: failed to append log: %s", err.Error())
	}

	s.LogsMu.Unlock()

	if args.LeaderCommit > s.CommitIndex {
		s.CommitIndex = min(args.LeaderCommit, s.CommitIndex)
	}
	if s.CommitIndex > s.LastApplied {
		s.ApplyLogCallback(s)
		s.LastApplied = s.CommitIndex
	}

	return s.Term, true
}

type RequestVoteArgs struct {
	Term         int  `json:"term"`
	CandidateId  Node `json:"candidate_id"`
	LastLogIndex int  `json:"last_log_idx"`
	LastLogTerm  int  `json:"last_log_term"`
}

// invoked by candiates
// Is thread safe.
func (s *State) RequestVote(args RequestVoteArgs) (int, bool) {
	if args.Term < s.Term {
		return s.Term, false
	}

	s.resetElectionTimeout <- signal
	// s.LogsMu.Lock()
	logLength, err := s.logLength()
	if err != nil {
		clog.Panicf("raft: vote request failed: failed to get log length: %s", err.Error())
	}
	logIndex := logLength - 1

	// s.LogsMu.Unlock()

	grantVote := (s.VotedFor == nil || *s.VotedFor == args.CandidateId) &&
		(args.Term >= s.Term && args.LastLogIndex >= logIndex)

	s.VotedFor = &args.CandidateId

	return s.Term, grantVote
}

// Is thread safe.
func (s *State) ToFollower() {
	clog.Info("[+] is now a follower")
	s.mode = ModeFollower

	select {
	case s.stopHeartbeatBroadcast <- signal:
		// message successfully sent
	default:
		// message dropped, probably its closed because no thing is running
	}
    // close(s.stopHeartbeatBroadcast)

	resetChan, stopChan := s.startElectionTimer()
	s.resetElectionTimeout = resetChan
	s.stopElectionTimer = stopChan
}

// Is thread safe.
func (s *State) ToCandidate() {
	clog.Info("[+] starting election")
	s.mode = ModeCandidate
	s.Term += 1
	s.VotedFor = &s.nodes[s.selfNode]

	s.resetElectionTimeout <- signal

	acquiredVotes := 0

	s.LogsMu.Lock()
	defer s.LogsMu.Unlock()

	selfNode := s.nodes[s.selfNode]

	lastLog, err := s.db.GetLastLog()

	if err == sql.ErrNoRows {
		lastLog = &Log{}
	} else if err != nil {
		clog.Panicf("raft: candidate process failed: failed to get log length: %s", err.Error())
	}

	payload := &RequestVoteArgs{
		Term:         s.Term,
		CandidateId:  selfNode,
		LastLogIndex: lastLog.Idx,
		LastLogTerm:  lastLog.Term,
	}

	for i, node := range s.nodes {
		if i == s.selfNode {
			continue
		}

		resp, err := s.SendRequestVote(node, payload)
		if err != nil {
			clog.Error("failed to send request vote: ", err)
			continue
		}
		if resp.Term > s.Term {
			s.ToFollower()
			return
		}
		if resp.VoteGranted {
            clog.Debug("vote acc")
			acquiredVotes += 1
		} else {
            clog.Debug("vote rejected")
        }
	}

	if hasMajorityVote(acquiredVotes, len(s.nodes)) {
		s.ToLeader()
	}
}

// ToLeader will:
// - disable service redirection,
// - disable election timer,
// - enable heartbeat broadcaster
// - reset []NextIndex and []MatchIndex
// Is thread safe.
func (s *State) ToLeader() {
	clog.Info("[+] is now a leader")
	s.mode = ModeLeader
	select {
	case s.stopElectionTimer <- signal:
		// message successfully sent
	default:
		// message dropped, probably its closed
	}
	// close(s.stopElectionTimer)
	// close(s.resetElectionTimeout)

	stopChan := s.startHeartbeatBroadcaster()
	s.stopHeartbeatBroadcast = stopChan

	s.NextIndex = make([]int, len(s.nodes))
	s.MatchIndex = make([]int, len(s.nodes))

	s.LogsMu.Lock()
	defer s.LogsMu.Unlock()

	logLength, err := s.logLength()
	if err != nil {
		clog.Panicf("raft: initiating leader failed: failed to get log length: %s", err.Error())
	}

	for i := range len(s.nodes) {
		s.NextIndex[i] = logLength
		s.MatchIndex[i] = -1
	}
}

// GetLogs will get list of logs from memory.
// Not thread safe.
func (s *State) GetLogs(start int, count int) ([]Log, error) {
	// l := s.Logs[start : start+count]
	l, err := s.db.ListLogs(start, count)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func hasMajorityVote(vote int, total int) bool {
	return vote > (total - vote)
}

func (s *State) BroadcastEntries(isHeartbeat bool) error {

	s.LogsMu.Lock()
	defer s.LogsMu.Unlock()

	logLength, err := s.logLength()
	if err != nil {
		return fmt.Errorf("raft: broadcast entries failed: failed to get log length: %s", err.Error())
	}

	leader := s.nodes[s.selfNode]
	for i, node := range s.nodes {

		if i == s.selfNode {
			continue
		}

		nextIdx := s.NextIndex[i]
		args := &AppendEntriesArgs{
			Term:     s.Term,
			LeaderId: leader,

			PrevLogIndex: nextIdx - 1,
			PrevLogTerm:  0,

			Entries:      nil,
			LeaderCommit: s.CommitIndex,
		}

		for {
			if !isHeartbeat {
				nextIdx = s.NextIndex[i]
				res, err := s.GetLogs(nextIdx-1, logLength-nextIdx)
				if err != nil {
					return err
				}

				args.PrevLogTerm = res[0].Term
				args.Entries = res[1:]
			}

			resp, err := s.SendAppendEntries(node, args)
			if err != nil {
				return err
			}

			if resp.Term > s.Term {
				s.ToFollower()
				return nil
			}

			if isHeartbeat {
				return nil
			}

			if resp.Success {
				s.NextIndex[i] = logLength
				s.MatchIndex[i] = logLength
				break
			}

			s.NextIndex[i] -= 1
		}
	}

	maxVal := 0
	for _, val := range s.MatchIndex {
		if maxVal < val {
			maxVal = val
		}
	}

	for val := maxVal; val > s.CommitIndex; val -= 1 {
		if val <= s.CommitIndex {
			continue
		}
		logData, err := s.db.GetLogByIdx(val)
		if err != nil {
			return err
		}
		if logData.Term != s.Term {
			continue
		}

		countTrue := 0
		for _, val2 := range s.MatchIndex {
			if val2 >= val {
				countTrue += 1
			}
		}

		if countTrue > (len(s.MatchIndex) - countTrue) {
			s.CommitIndex = val
			break
		}
	}

	return nil
}
