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
	Idx   int    `json:"idx"  db:"log_idx"`
	Term  int    `json:"term" db:"log_term"`
	Op    int    `json:"op"   db:"op"`
	Key   string `json:"key"  db:"strkey"`
	Value string `json:"val"  db:"strval"`
}

type NodeStatus int

const (
	NodeUnreachable NodeStatus = iota
	NodeActive
	NodeLeader
)

type Node struct {
	// ID     int
	Addr     string `db:"address"`
	RestAddr string `db:"rest_address"`
	Status   NodeStatus
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

	// leaderIdx int
	leaderNode *Node
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
	clog.Logger.SetLevel(
		log.InfoLevel,
		// log.DebugLevel,
	)

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
			Addr:     node.Addr,
			RestAddr: node.RestAddr,
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

		// leaderIdx: -1,
		leaderNode: nil,
	}
}

func (s *State) Run() {
	s.ToFollower()
	s.StartServer()
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
				clog.Debugf("Resetting election timer")
			case <-time.After(s.electionTimeout):
				go s.ToCandidate()
			}
		}
		clog.Info("ELECTION STOPPED")
	}()

	return reset, stop
}

// Is thread safe.
func (s *State) startHeartbeatBroadcaster() chan any {
	clog.Debugf("BROADCASTING YOOOO %d", s.Term)
	stop := make(chan any, 10)

	go func() {
		clog.Debugf("started BROADCASTING with interval: %v", s.heartbeatInterval)

	broadcastLoop:
		for {
			select {
			case <-stop:
				break broadcastLoop
			case <-time.After(s.heartbeatInterval):
				clog.Debug("broadcasting...")
				err := s.BroadcastEntries()
				if err != nil {
					clog.Errorf("broadcast failed: %v", err)
				}
			}
		}
		clog.Debugf("Broadcast STOPPED")
	}()

	return stop
}

func (s *State) IsLeader() bool {
	s.lock.Lock()
	clog.Debugf("isleader hold the lock")
	defer s.lock.Unlock()

	return s.mode == ModeLeader
}

func (s *State) GetLeaderAddr() string {
	s.lock.Lock()
	clog.Debugf("getleaderaddr hold the lock")
	defer s.lock.Unlock()

	// return s.nodes[s.leaderIdx].Addr
	return s.leaderNode.RestAddr
}

type AppendEntriesArgs struct {
	Term         int   `json:"term"`
	LeaderId     int   `json:"leader_id"`
	PrevLogIndex int   `json:"prev_log_idx"`
	PrevLogTerm  int   `json:"prev_log_term"`
	Entries      []Log `json:"entries"`
	LeaderCommit int   `json:"leader_commit"`
}

// invoked by leader
// Is thread safe.
func (s *State) AppendEntries(args AppendEntriesArgs) (int, bool) {
	clog.Debug("appending entry")

	s.lock.Lock()
	clog.Debugf("appendentries hold the lock")
	defer s.lock.Unlock()

	s.resetElectionTimeout <- signal

    if args.Term < s.Term {
        return s.Term, false
    }

	if args.Term > s.Term {
		s.Term = args.Term

		err := s.conn.SetKey(KEY_TERM, s.Term)
		if err != nil {
			clog.Panicf("raft: failed to set term from db: %v", err)
		}

		s.ToFollower()
		clog.Debug("setting new id to ", args.LeaderId)

		s.leaderNode = &s.nodes[args.LeaderId]

		s.VotedFor = nil
		err = s.conn.SetKey(KEY_VOTEDFOR, "")
		if err != nil {
			clog.Panicf("raft: failed to set votedfor from db: %v", err)
		}
	}


	prevLog, err := s.conn.GetLogByIdx(args.PrevLogIndex)
	if err != nil {
		clog.Panicf("raft: failed to get log length: %s", err.Error())
	}

    // check for conflict
	if prevLog == nil || prevLog.Term != args.PrevLogTerm {
		return s.Term, false
	}

    // return early if there's no entry to append
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
	clog.Debugf("requestvote hold the lock")
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
    clog.Infof("voted for %s", s.VotedFor.Addr)
	err = s.conn.SetKey(KEY_VOTEDFOR, s.VotedFor.Addr)
	if err != nil {
		clog.Panicf("raft: failed to set votedfor from db: %v", err)
	}

	s.leaderNode = &args.CandidateId

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
		clog.Errorf("success to stop heart beat")
	default:
		// message dropped, probably its closed because no thing is running
		clog.Errorf("failed to stop heart beat")
	}
	// close(s.stopHeartbeatBroadcast)

	resetChan, stopChan := s.startElectionTimer()
	s.resetElectionTimeout = resetChan
	s.stopElectionTimer = stopChan
}

// Is thread safe.
func (s *State) ToCandidate() {
	clog.Info("is now a candidate")

	s.lock.Lock()
	clog.Debugf("tocandidate hold the lock")
	defer s.lock.Unlock()

	s.mode = ModeCandidate
	s.Term += 1
	s.VotedFor = &s.nodes[s.selfNode]
	s.resetElectionTimeout <- signal

	clog.Debugf("setting term")

	err := s.conn.SetKey(KEY_TERM, s.Term)
	if err != nil {
		clog.Panicf("raft: failed to set term from db: %v", err)
	}

    clog.Infof("voted for %s", s.VotedFor.Addr)
	clog.Debugf("setting votedfor")
	err = s.conn.SetKey(KEY_VOTEDFOR, s.VotedFor.Addr)
	if err != nil {
		clog.Panicf("raft: failed to set votedfor from db: %v", err)
	}

	clog.Info("[+] starting election with term", s.Term)

	acquiredVotes := 0
	selfNode := s.nodes[s.selfNode]
	lastLog, err := s.conn.GetLastLog()

	if err == sql.ErrNoRows || (lastLog == nil && err == nil) {
		lastLog = &Log{
			Idx:  -1,
			Term: s.Term,
		}
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
		clog.Debugf("send request to node %d", i)

		if i == s.selfNode {
			continue
		}
		clog.Debugf("not self node")
		resp, err := s.SendRequestVote(node, payload)
		if err != nil {
			clog.Error("failed to send request vote: ", err)
			continue
		}
		clog.Debugf("done send")
		if resp.Term > s.Term {
			s.ToFollower()
			// s.leaderIdx = i
			s.leaderNode = &s.nodes[i]
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
		clog.Errorf("successfully stop election timer")
	default:
		// message dropped, probably its closed
		clog.Errorf("failed to stop election timer")
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
	clog.Debugf("insertlog hold the lock")
	defer s.lock.Unlock()

	l.Term = s.Term

	return s.conn.InsertLogs([]Log{l})
}

func (s *State) BroadcastEntries() error {

	s.lock.Lock()
	clog.Debugf("broadcastentry hold the lock")
	defer s.lock.Unlock()

	logLength, err := s.logLength()
	if err != nil {
		return fmt.Errorf("raft: broadcast entries failed: failed to get log length: %s", err.Error())
	}

	for i := range s.nodes {
		node := s.nodes[i]
		clog.Debugf("sending to %s", node.Addr)

		if i == s.selfNode {
			continue
		}

		clog.Debug("stop 1")
		nextIdx := s.NextIndex[i]
		args := &AppendEntriesArgs{
			Term:     s.Term,
			LeaderId: s.selfNode,

			PrevLogIndex: nextIdx - 1,
			PrevLogTerm:  0,

			Entries:      nil,
			LeaderCommit: s.CommitIndex,
		}

		clog.Debug("stop 2.1")
		nextIdx = s.NextIndex[i]

		if logLength-nextIdx > 0 {

			res, err := s.GetLogs(nextIdx-1, logLength-nextIdx)
			if err != nil {
				return err
			}

			clog.Infof("get log param: %d %d %d %d",
				nextIdx-1,
				logLength-nextIdx,
				nextIdx,
				logLength,
			)
			clog.Debug("stop 2.5")

			args.PrevLogTerm = res[0].Term
			args.Entries = res[1:]
		} else {
			args.PrevLogTerm = s.Term
			args.Entries = []Log{}
		}

		clog.Debug("stop 3")
		resp, err := s.SendAppendEntries(node, args)
		if err != nil {
			clog.Errorf("failed to send AppendEntries to %v: %v", node, err)
			continue
		}

		clog.Debug("stop 4")
		if resp.Term > s.Term {
			s.ToFollower()
			s.leaderNode = &s.nodes[i]
			return nil
		}

		clog.Debug("stop 6")
		if resp.Success {
			s.NextIndex[i] = logLength
			s.MatchIndex[i] = logLength
			continue
		}

		clog.Debug("stop 7")
        if s.NextIndex[i] > 0 {
            s.NextIndex[i] -= 1
        }
		clog.Debugf("wtf")
	}

	clog.Debug("stop 8")
	maxVal := 0
	for _, val := range s.MatchIndex {
		if maxVal < val {
			maxVal = val
		}
	}

	clog.Debug("stop 9")
	for val := maxVal; val > s.CommitIndex; val -= 1 {
		clog.Debug("stop 9.1")
		if val <= s.CommitIndex {
			continue
		}
		logData, err := s.conn.GetLogByIdx(val)
		if err != nil {
			return err
		}

		clog.Debug("stop 9.2")
		if logData == nil || logData.Term != s.Term {
			continue
		}

		clog.Debug("stop 9.3")
		countTrue := 0
		for _, val2 := range s.MatchIndex {
			if val2 >= val {
				countTrue += 1
			}
		}

		clog.Debug("stop 9.4")
		if countTrue > (len(s.MatchIndex) - countTrue) {
			s.CommitIndex = val
			break
		}
	}

	return nil
}
