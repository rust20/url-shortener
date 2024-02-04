package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	netURL "net/url"

	_ "github.com/mattn/go-sqlite3"
	"gitlab.db.in.tum.de/rust20/url-shortener/raft"

	"crypto/md5"
	"encoding/base64"
	"encoding/json"
)

var ErrNoSuchID = fmt.Errorf("no such id")
var ErrDecodeID = fmt.Errorf("malformed or invalid id")
var ErrURLTooLong = fmt.Errorf("URL is too long")
var ErrInvalidURL = fmt.Errorf("URL in request is invalid")
var ErrInvalidRequest = fmt.Errorf("request is invalid")

const ID_LENGHT = 8
const MAX_URL_LENGTH = 1024
const DB_FILE = "store.db"

type serverOp int

const (
	SOpAdd serverOp = iota
	SOpDel
)

type server struct {
	dbConn   *sql.DB
	urlToId  map[string]string
	idToUrl  map[string]string
	idLength int32

	raftServer *raft.State
}

type ShortURLResponse struct {
	Status   int32  `json:"status"`
	ShortURL string `json:"short_url,omitempty"`
	URL      string `json:"URL,omitempty"`
	Message  string `json:"message"`
}

func (s *server) handler(w http.ResponseWriter, req *http.Request) {
	log.Printf("[+] path: %s | method: %s\n", req.URL.Path, req.Method)
	w.Header().Set("Content-Type", "application/json")

	// TODO: redirect if not leader

	if req.Method == "POST" {
		req.ParseForm()
		urls, ok := req.PostForm["url"]
		if !ok || len(urls) == 0 {
			log.Println("invalid request")
			http.Error(w, ErrInvalidRequest.Error(), http.StatusBadRequest)
			return
		}
		url := urls[0]
		if !validateURL(url) {
			log.Println("invalid url")
			http.Error(w, ErrInvalidURL.Error(), http.StatusBadRequest)
			return
		}
		res, err := s.CreateShortURL(url)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		s.idToUrl[res] = url
		s.urlToId[url] = res

		response := ShortURLResponse{
			Status:   http.StatusAccepted,
			ShortURL: res,
			Message:  "success",
		}

		responseJson, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		w.Write(responseJson)
		return

	} else if req.Method == "GET" {
		if len(req.URL.Path) < 1 {
			http.Error(w, ErrInvalidRequest.Error(), http.StatusBadRequest)
			return
		}

		url_id := req.URL.Path[1:]
		url, err := s.GetShortURL(url_id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		http.Redirect(w, req, url, http.StatusSeeOther)
		return
	}
}

func (s *server) CreateShortURL(url string) (string, error) {
	if len(url) > MAX_URL_LENGTH {
		return "", ErrURLTooLong
	}

	urlId := ""
	if val, ok := s.urlToId[url]; ok {
		urlId = val
	} else {
		newId := s.HashURL(url)
		s.urlToId[url] = newId
		s.idToUrl[newId] = url
		urlId = newId
	}

	err := s.raftServer.AddLog(raft.Log{
		Op:    int(SOpAdd),
		Key:   urlId,
		Value: url,
	})
	if err != nil {
		return "", err
	}
	err = s.raftServer.BroadcastEntries(false)
	if err != nil {
		return "", err
	}

	return urlId, nil
}

func (s *server) GetShortURL(url_id string) (string, error) {
	if val, ok := s.idToUrl[url_id]; ok {
		return val, nil
	}
	return "", ErrNoSuchID
}

func (s *server) HashURL(url string) string {
	hasher := md5.New()
	hasher.Write([]byte(url))

	encoded := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
	return encoded[:s.idLength]
}

func (s *server) InsertShortURL(url_id string, url_full string) {
    s.urlToId[url_full] = url_id
    s.idToUrl[url_id] = url_full
}

func (s *server) ApplyLogs(self *raft.State) {
	logs := self.Logs[self.LastApplied:self.CommitIndex]
	for _, log := range logs {
		url_id := log.Key
		url_full := log.Value
		if log.Op == int(SOpAdd) {
            s.InsertShortURL(url_id, url_full)
		} else if log.Op == int(SOpDel) {
			delete(s.urlToId, url_full)
			delete(s.idToUrl, url_id)
		}
	}
}

func validateURL(url string) bool {
	_, err := netURL.ParseRequestURI(url)
	return err == nil
}

func main() {
	dbConn, err := sql.Open("sqlite3", DB_FILE)
	if err != nil {
		log.Fatalf("failed to open database connection: %s", err.Error())
	}

	svr := &server{
		dbConn:   dbConn,
		urlToId:  map[string]string{},
		idToUrl:  map[string]string{},
		idLength: ID_LENGHT,

		raftServer: nil,
	}

	raftServer := raft.NewState(dbConn, &raft.NewStateArgs{
		ApplyLog: svr.ApplyLogs,
		ElectionTimeout: 0,
		SelfNode:        raft.Node{},
	})

	svr.raftServer = raftServer

	http.HandleFunc("/", svr.handler)
	log.Println("running")

	http.ListenAndServe(":8080", nil)
}
