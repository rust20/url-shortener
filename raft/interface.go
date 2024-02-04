package raft

import "net/http"

type Server interface {
	HandleAppendEntries(w http.ResponseWriter, req *http.Request)
	HandleRequestVote(w http.ResponseWriter, req *http.Request)
	SendAppendEntries(target Node, args *AppendEntriesArgs) (*AppendEntriesResponse, error)
	SendRequestVote(target Node, args *RequestVoteArgs) (*RequestVoteResponse, error)

	AddLog(log Log) error
	BroadcastEntries(isHeartbeat bool) error
}
