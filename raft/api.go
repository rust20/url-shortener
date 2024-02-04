package raft

import (
	"encoding/json"
	"fmt"
	"net/http"

	log "github.com/sirupsen/logrus"
)

const RequestVoteAPIPath = "/requestvote"
const AppendEntriesAPIPath = "/appendentries"

type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

func (s *State) HandleAppendEntries(w http.ResponseWriter, req *http.Request) {
	log.Printf("[!] append entries")

	if req.Method != "POST" {
		http.Error(w, "invalid method", http.StatusBadRequest)
		return
	}

	args := AppendEntriesArgs{}

	err := json.NewDecoder(req.Body).Decode(&args)
	if err != nil {
		message := fmt.Sprintf("failed to parse body: %s", err)
		http.Error(w, message, http.StatusBadRequest)
		return
	}

	term, ok := s.AppendEntries(args)

	resp := AppendEntriesResponse{
		Term:    term,
		Success: ok,
	}

	responseJson, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(responseJson)
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

func (s *State) HandleRequestVote(w http.ResponseWriter, req *http.Request) {
	log.Info("[!] request vote")

	if req.Method != "POST" {
		http.Error(w, "invalid method", http.StatusBadRequest)
		return
	}

	args := RequestVoteArgs{}

	err := json.NewDecoder(req.Body).Decode(&args)
	if err != nil {
		message := fmt.Sprintf("failed to parse body: %s", err)
		http.Error(w, message, http.StatusBadRequest)
		return
	}

	term, ok := s.RequestVote(args)

	resp := RequestVoteResponse{
		Term:        term,
		VoteGranted: ok,
	}

	responseJson, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(responseJson)
}

func (s *State) SendAppendEntries(target Node, args *AppendEntriesArgs) (*AppendEntriesResponse, error) {
	// TODO: impl retry mechanism

	targetURL := fmt.Sprintf("%s:%d%s", target.Address, target.Port, AppendEntriesAPIPath)
	req, err := http.NewRequest(http.MethodPost, targetURL, nil)

	if err != nil {
		return nil, err
	}

	reqResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	resp := &AppendEntriesResponse{}
	err = json.NewDecoder(reqResp.Body).Decode(resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *State) SendRequestVote(target Node, args *RequestVoteArgs) (*RequestVoteResponse, error) {
	// TODO: impl retry mechanism

	targetURL := fmt.Sprintf("%s:%d%s", target.Address, target.Port, RequestVoteAPIPath)
	req, err := http.NewRequest(http.MethodPost, targetURL, nil)
	if err != nil {
		return nil, err
	}

	reqResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	resp := &RequestVoteResponse{}
	err = json.NewDecoder(reqResp.Body).Decode(resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *State) AddLog(log Log) error {
	s.LogsMu.Lock()
	defer s.LogsMu.Unlock()

    log.Term = s.CurrentTerm
    log.Idx = s.logLength()

    s.Logs = append(s.Logs, log)
    // TODO: append in db as well

    return nil
}

func (s *State) BroadcastEntries(isHeartbeat bool) error {
    // TODO: maybe add somekind of buffer?

	s.LogsMu.Lock()
	defer s.LogsMu.Unlock()

    logLength := s.logLength()

	for i, node := range s.nodes {
		if node == s.selfNode {
			continue
		}

		nextIdx := s.NextIndex[i]
		args := &AppendEntriesArgs{
			Term:     s.CurrentTerm,
			LeaderId: s.selfNode,

			PrevLogIndex: nextIdx - 1,
			PrevLogTerm:  s.Logs[nextIdx - 1].Term,

			Entries:      s.Logs,
			LeaderCommit: s.CommitIndex,
		}

		if !isHeartbeat {
			// TODO: change to fetch logs from db(?)
			// res, err := s.GetLogs(int(s.NextIndex[i]), int(s.logLength() - s.logLength()))
			res := s.Logs[nextIdx:logLength]
			args.Entries = res
		}

		// TODO: make retryable
		// TODO: do this in a goroutine
		resp, err := s.SendAppendEntries(node, args)
        // TODO: retry if unreachable
		if err != nil {
			return err
		}

		if resp.Term > s.CurrentTerm {
			s.ToFollower()
			return nil
		}

		if isHeartbeat {
			return nil
		}

		if resp.Success {
			s.NextIndex[i] = logLength
			s.MatchIndex[i] = logLength
		}
	}

	return nil
}
