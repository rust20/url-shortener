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
const KEY_TERM = "term"
const KEY_VOTEDFOR = "votedfor"

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
	lock sync.Mutex

	mode ModeState

	// general persistent state
	// update before respond to rpcs
	Term     int
	VotedFor *Node

	// general volatile state
	CommitIndex int
	LastApplied int

	// leader's volatile state
	// reset after elections

	NextIndex  []int
	MatchIndex []int

	ApplyLogCallback func(self *State)

	electionTimeout      time.Duration
	resetElectionTimeout chan any
	stopElectionTimer    chan any

	heartbeatInterval      time.Duration
	stopHeartbeatBroadcast chan any

	nodes    []Node
	selfNode int

	conn    *database
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
	clog.Logger.SetLevel(log.InfoLevel)

	randResult := rand.Intn(args.Config.ElectionTimeoutRange)
	timeoutOffset := time.Duration(randResult*100) * time.Millisecond

	clog.Info("timout offset -> ", timeoutOffset)

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

	term, err := conn.GetValInt(KEY_TERM)
	if err == sql.ErrNoRows {
		term = 0
	} else if err != nil {
		clog.Panicf("raft: failed to get term from db: %v", err)
	}

	var votedFor *Node = nil
	votedForAddr, _ := conn.GetValString(KEY_VOTEDFOR)

	if err != nil && err != sql.ErrNoRows {
		clog.Panicf("raft: failed to get votedfor from db: %v", err)
	}

	if votedForAddr != "" {
		votedFor = &Node{Addr: votedForAddr}
	}

	return &State{

		mode:     ModeFollower,
		Term:     term,
		VotedFor: votedFor,

		CommitIndex: -1,
		LastApplied: -1,
		NextIndex:   []int{},
		MatchIndex:  []int{},

		ApplyLogCallback:     args.ApplyLog,
		electionTimeout:      electionTimeout,
		resetElectionTimeout: make(chan any),
		stopElectionTimer:    make(chan any),

		heartbeatInterval: args.Config.HeartbeatInterval,

		stopHeartbeatBroadcast: make(chan any),
		nodes:                  nodes,
		selfNode:               args.SelfNode,

		conn: conn,
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
	return s.conn.LogsLength()
}

// Is thread safe.
func (s *State) startElectionTimer() (chan any, chan any) {
	reset := make(chan any, 10)
	stop := make(chan any, 10)

	go func() {
	electionLoop:
		for {
			select {
			case <-stop:
				break electionLoop
			case <-reset:
				log.Debugf("Resetting election timer")
			case <-time.After(s.electionTimeout):
				go s.ToCandidate()
			}
		}
		log.Info("ELECTION STOPPED")
	}()

	return reset, stop
}

// Is thread safe.
func (s *State) startHeartbeatBroadcaster() chan any {
	clog.Debugf("BROADCASTING YOOOO %d", s.Term)
	stop := make(chan any, 10)

	go func() {
		clog.Infof("started BROADCASTING YOO with interval: %v", s.heartbeatInterval)

	broadcastLoop:
		for {
			select {
			case <-stop:
				break broadcastLoop
			case <-time.After(s.heartbeatInterval):
				clog.Debug("broadcasting...")
				err := s.BroadcastEntries()
				if err != nil {
					log.Errorf("broadcast failed: %v", err)
				}
			}
		}
		log.Debugf("Broadcast STOPPED")
	}()

	return stop
}

// invoked by leader
// Is thread safe.
func (s *State) AppendEntries(args AppendEntriesArgs) (int, bool) {

	s.lock.Lock()
	defer s.lock.Unlock()

	if args.Term > s.Term {

		if s.mode == ModeLeader {
			s.Term = args.Term

			err := s.conn.SetKey(KEY_TERM, s.Term)
			if err != nil {
				clog.Panicf("raft: failed to set term from db: %v", err)
			}

			s.ToFollower()
		}
		s.VotedFor = nil
		err := s.conn.SetKey(KEY_VOTEDFOR, "")
		if err != nil {
			clog.Panicf("raft: failed to set votedfor from db: %v", err)
		}
	}

	s.resetElectionTimeout <- signal

	logLength, err := s.logLength()
	if err != nil {
		clog.Panicf("raft: failed to get log length")
	}

	if logLength < args.PrevLogIndex {
		return s.Term, false
	}

	prevLog, err := s.conn.GetLogByIdx(args.PrevLogIndex)
	if err != nil {
		clog.Panicf("raft: failed to get log length: %s", err.Error())
	}

	if prevLog != nil && prevLog.Term != args.PrevLogTerm {
		return s.Term, false
	}

	if len(args.Entries) == 0 {
		return s.Term, true
	}

	logs, err := s.conn.ListLogs(args.PrevLogIndex+1, len(args.Entries))

	deleteFrom := 0

	for i, entry := range args.Entries {
		if entry.Term != logs[entry.Idx-args.PrevLogIndex].Term {
			deleteFrom = i
			break
		}
	}

	err = s.conn.DeleteLogs(args.Entries[deleteFrom].Idx)
	if err != nil {
		clog.Panicf("raft: failed to delete log: %s", err.Error())
	}

	err = s.conn.InsertLogs(args.Entries[deleteFrom:])
	if err != nil {
		clog.Panicf("raft: failed to append log: %s", err.Error())
	}

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

	s.lock.Lock()
	defer s.lock.Unlock()

	if args.Term < s.Term {
		return s.Term, false
	}

	s.resetElectionTimeout <- signal
	logLength, err := s.logLength()
	if err != nil {
		clog.Panicf("raft: vote request failed: failed to get log length: %s", err.Error())
	}
	logIndex := logLength - 1

	grantVote := (s.VotedFor == nil || *s.VotedFor == args.CandidateId) &&
		(args.Term >= s.Term && args.LastLogIndex >= logIndex)

	s.VotedFor = &args.CandidateId
	err = s.conn.SetKey(KEY_VOTEDFOR, s.VotedFor.Addr)
	if err != nil {
		clog.Panicf("raft: failed to set votedfor from db: %v", err)
	}

	s.Term = args.Term
	err = s.conn.SetKey(KEY_TERM, s.Term)
	if err != nil {
		clog.Panicf("raft: failed to set term from db: %v", err)
	}

	return s.Term, grantVote
}

// Is thread safe.
func (s *State) ToFollower() {
	clog.Info("[+] is now a follower")
	s.mode = ModeFollower

	select {
	case s.stopHeartbeatBroadcast <- signal:
		// message successfully sent
		log.Errorf("success to stop heart beat")
	default:
		// message dropped, probably its closed because no thing is running
		log.Errorf("failed to stop heart beat")
	}
	// close(s.stopHeartbeatBroadcast)

	resetChan, stopChan := s.startElectionTimer()
	s.resetElectionTimeout = resetChan
	s.stopElectionTimer = stopChan
}

// Is thread safe.
func (s *State) ToCandidate() {

	s.lock.Lock()
	defer s.lock.Unlock()

	s.mode = ModeCandidate
	s.Term += 1
	s.VotedFor = &s.nodes[s.selfNode]
	s.resetElectionTimeout <- signal

	err := s.conn.SetKey(KEY_TERM, s.Term)
	if err != nil {
		clog.Panicf("raft: failed to set term from db: %v", err)
	}

	err = s.conn.SetKey(KEY_VOTEDFOR, s.VotedFor.Addr)
	if err != nil {
		clog.Panicf("raft: failed to set votedfor from db: %v", err)
	}

	clog.Info("[+] starting election with term", s.Term)

	acquiredVotes := 0
	selfNode := s.nodes[s.selfNode]
	lastLog, err := s.conn.GetLastLog()

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

	clog.Debug("requesting votes")

	for i := range s.nodes {

		node := s.nodes[i]

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

	clog.Debug("done requesting vote")

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

	s.VotedFor = nil
	err := s.conn.SetKey(KEY_VOTEDFOR, "")
	if err != nil {
		clog.Panicf("raft: failed to set votedfor from db: %v", err)
	}

	select {
	case s.stopElectionTimer <- signal:
		// message successfully sent
		log.Errorf("success to stop election")
	default:
		// message dropped, probably its closed
		log.Errorf("failed to stop election")
	}
	// close(s.stopElectionTimer)
	// close(s.resetElectionTimeout)

	stopChan := s.startHeartbeatBroadcaster()
	s.stopHeartbeatBroadcast = stopChan

	s.NextIndex = make([]int, len(s.nodes))
	s.MatchIndex = make([]int, len(s.nodes))

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

	l, err := s.conn.ListLogs(start, count)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func hasMajorityVote(vote int, total int) bool {
	return vote > (total - vote)
}

func (s *State) InsertLog(l Log) error {
	s.lock.Lock()
	defer s.lock.Unlock()

    l.Term = s.Term

	return s.conn.InsertLogs([]Log{l})
}

func (s *State) BroadcastEntries() error {

	s.lock.Lock()
	defer s.lock.Unlock()

	logLength, err := s.logLength()
	if err != nil {
		return fmt.Errorf("raft: broadcast entries failed: failed to get log length: %s", err.Error())
	}

	leader := s.nodes[s.selfNode]
	for i := range s.nodes {
		node := s.nodes[i]
		log.Debugf("sending to %s", node.Addr)

		if i == s.selfNode {
			continue
		}

		log.Debug("stop 1")
		nextIdx := s.NextIndex[i]
		args := &AppendEntriesArgs{
			Term:     s.Term,
			LeaderId: leader,

			PrevLogIndex: nextIdx - 1,
			PrevLogTerm:  0,

			Entries:      nil,
			LeaderCommit: s.CommitIndex,
		}

		log.Debug("stop 2.1")
		nextIdx = s.NextIndex[i]

		if logLength-nextIdx > 0 {

			res, err := s.GetLogs(nextIdx-1, logLength-nextIdx)
			if err != nil {
				return err
			}
			log.Debug("stop 2.5")

			args.PrevLogTerm = res[0].Term
			args.Entries = res[1:]
		}

		log.Debug("stop 3")
		resp, err := s.SendAppendEntries(node, args)
		if err != nil {
			log.Errorf("failed to send AppendEntries to %v: %v", node, err)
			continue
		}

		log.Debug("stop 4")
		if resp.Term > s.Term {
			s.ToFollower()
			return nil
		}

		log.Debug("stop 6")
		if resp.Success {
			s.NextIndex[i] = logLength
			s.MatchIndex[i] = logLength
			continue
		}

		log.Debug("stop 7")
		s.NextIndex[i] -= 1
		log.Debugf("wtf")
	}

	log.Debug("stop 8")
	maxVal := 0
	for _, val := range s.MatchIndex {
		if maxVal < val {
			maxVal = val
		}
	}

	log.Debug("stop 9")
	for val := maxVal; val > s.CommitIndex; val -= 1 {
		log.Debug("stop 9.1")
		if val <= s.CommitIndex {
			continue
		}
		logData, err := s.conn.GetLogByIdx(val)
		if err != nil {
			return err
		}

		log.Debug("stop 9.2")
		if logData == nil || logData.Term != s.Term {
			continue
		}

		log.Debug("stop 9.3")
		countTrue := 0
		for _, val2 := range s.MatchIndex {
			if val2 >= val {
				countTrue += 1
			}
		}

		log.Debug("stop 9.4")
		if countTrue > (len(s.MatchIndex) - countTrue) {
			s.CommitIndex = val
			break
		}
	}

	return nil
}
