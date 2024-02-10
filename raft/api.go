package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	// log "github.com/sirupsen/logrus"
)

const RequestVoteAPIPath = "/requestvote"
const AppendEntriesAPIPath = "/appendentries"

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

func (s *State) StartServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /requestvote", s.HandleRequestVote)
	mux.HandleFunc("POST /appendentries", s.HandleAppendEntries)

	selfNode := s.nodes[s.selfNode]

	err := http.ListenAndServe(selfNode.Addr, mux)
	if err != nil {
		clog.Panicf("raft server failed: %s", err.Error())
	}
}

func (s *State) HandleAppendEntries(w http.ResponseWriter, req *http.Request) {
	clog.Debugf("[!] append entries")

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
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

func (s *State) HandleRequestVote(w http.ResponseWriter, req *http.Request) {
	clog.Debugf("[!] request vote")

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
	clog.Debugf("[>>] send append entries")
	// TODO: impl retry mechanism

	jsonBody, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	targetURL := fmt.Sprintf("http://%s%s", target.Addr, AppendEntriesAPIPath)
	req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

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
	clog.Debugf("[>>] send request vote")
	// TODO: impl retry mechanism

	jsonBody, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	targetURL := fmt.Sprintf("http://%s%s", target.Addr, RequestVoteAPIPath)
	req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	if err != nil {
		clog.Debug("wawawa")
		return nil, err
	}

	reqResp, err := http.DefaultClient.Do(req)
	if err != nil {
		clog.Debug("wawi1")
		return nil, err
	}

	// content, _ := io.ReadAll(reqResp.Body)
	//
	// log.Debug("raw contnet", string(content))
	//
	resp := &RequestVoteResponse{}
	err = json.NewDecoder(reqResp.Body).Decode(resp)
	if err != nil {
		clog.Debug("wawi2")
		return nil, err
	}

	return resp, nil
}
