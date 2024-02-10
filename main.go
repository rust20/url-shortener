package main

import (
	"flag"
	"fmt"
	"net/http"
	netURL "net/url"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"gitlab.db.in.tum.de/rust20/url-shortener/config"
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
var ErrNotFound = fmt.Errorf("entity not found")

const ID_LENGHT = 8
const MAX_URL_LENGTH = 20000
const DB_FILE = "store.db"
const CheckInvalidURL = false

type serverOp int

const (
	SOpAdd serverOp = iota
	SOpDel
)

type server struct {
	dbConn *sqlx.DB

	urlMu sync.Mutex
	db    *database

	raftServer *raft.State
}

type ShortURLResponse struct {
	Status   int32  `json:"status"`
	ShortURL string `json:"short_url,omitempty"`
	URL      string `json:"URL,omitempty"`
	Message  string `json:"message"`
}

func (s *server) getHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("[+] GET | path: %s \n", req.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	// TODO: redirect if not leader
	if len(req.URL.Path) < 1 {
		http.Error(w, ErrInvalidRequest.Error(), http.StatusBadRequest)
		return
	}

	url_id := req.PathValue("id")

	// url_id := req.URL.Path[1:]
	// url, err := s.GetShortURL(url_id)
	url, err := s.db.GetShortURL(url_id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	http.Redirect(w, req, url, http.StatusSeeOther)
	return
}

func (s *server) postHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("[+] POST | path: %s \n", req.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	// TODO: redirect if not leader

	req.ParseForm()
	urls, ok := req.PostForm["url"]
	if !ok || len(urls) == 0 {
		log.Errorf("invalid request")
		http.Error(w, ErrInvalidRequest.Error(), http.StatusBadRequest)
		return
	}
	url := urls[0]
	// log.Println("url value", url)
	if CheckInvalidURL && !validateURL(url) {
		log.Errorln("invalid url")
		http.Error(w, ErrInvalidURL.Error(), http.StatusBadRequest)
		return
	}
	res, err := s.CreateShortURL(url)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	storedURLId, err := s.db.GetShortURL(res)
	if err == ErrNotFound {
		http.Error(w, "invalid id: not found", http.StatusNotFound)
		return
	}

	if err != nil && err != ErrNotFound {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if storedURLId != "" {
		log.Debug("hit")
		response := ShortURLResponse{
			Status:   http.StatusAccepted,
			ShortURL: storedURLId,
			Message:  "success",
		}

		responseJson, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		w.Write(responseJson)
		return
	}

	err = s.db.InsertShortURL(res, url)
	if err != nil {
		log.Errorf("server error inserting: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = s.raftServer.BroadcastEntries(false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// s.idToUrl[res] = url
	// s.urlToId[url] = res

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
}

func (s *server) CreateShortURL(url string) (string, error) {
	if len(url) > MAX_URL_LENGTH {
		return "", ErrURLTooLong
	}

	urlId := ""

	// if val, ok := s.urlToId[url]; ok {
	// 	urlId = val
	// } else {
	// 	newId := s.HashURL(url)
	// 	// s.urlToId[url] = newId
	// 	// s.idToUrl[newId] = url
	// 	urlId = newId
	// }

	// err := s.raftServer.AddLog(raft.Log{
	// 	Op:    int(SOpAdd),
	// 	Key:   urlId,
	// 	Value: url,
	// })
	// if err != nil {
	// 	return "", err
	// }
	// err = s.raftServer.BroadcastEntries(false)
	// if err != nil {
	// 	return "", err
	// }

	return urlId, nil
}

func (s *server) HashURL(url string) string {
	hasher := md5.New()
	hasher.Write([]byte(url))

	encoded := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
	return encoded[:ID_LENGHT]
}

func (s *server) ApplyLogs(self *raft.State) {
	// TODO: fix
	// logs := self.Logs[self.LastApplied:self.CommitIndex]

	logs, err := self.GetLogs(self.LastApplied, self.CommitIndex-self.LastApplied)
	if err != nil {
		log.Panicf("raft: apply log fail, unable to get logs: %s", err.Error())
	}
	for _, dataLog := range logs {
		url_id := dataLog.Key
		url_full := dataLog.Value
		if dataLog.Op == int(SOpAdd) {
			err := s.db.InsertShortURL(url_id, url_full)
			if err != nil {
				log.Panicf("server error inserting: %s", err.Error())
			}
		} else if dataLog.Op == int(SOpDel) {
			err := s.db.DeleteShortURL(url_id)
			if err != nil {
				log.Panicf("server error deleting: %s", err.Error())
			}
		}
	}
}

func validateURL(url string) bool {
	_, err := netURL.ParseRequestURI(url)
	return err == nil
}

type CommandArgs struct {
	ConfigName string

	Self  int
	Nodes config.ServerConfig // probably dont need this, since the

	HeartbeatInterval    time.Duration
	ElectionTimeoutBase  int
	ElectionTimeoutRange int

    RestServer string

	dbPrefix string
	dbSuffix string
}

func InitCommandArgs() CommandArgs {
	cmdArgs := CommandArgs{}

	flag.StringVar(&cmdArgs.ConfigName, "c", "config.yaml", "config file name")

	// flag.StringVar(&cmdArgs.Self.Addr, "raft-addr", "127.0.0.1", "address of current server") // TODO: get automatically
	// flag.StringVar(&cmdArgs.Self.Port, "raft-port", "1337", "port of raft server")            // TODO: get automatically
	flag.IntVar(&cmdArgs.Self, "self-id", 0, "index of current id from list of nodes defined in config")


	flag.DurationVar(&cmdArgs.HeartbeatInterval, "hb", 500*time.Millisecond, "heartbeat interval duration value")
	flag.IntVar(&cmdArgs.ElectionTimeoutBase, "et-base", 500, "base value of election timeout")
	flag.IntVar(&cmdArgs.ElectionTimeoutRange, "et-range", 300, "range value of randomized election timeout")

	// TODO: check that the election timeout have to be longer than heartbeat interval

	flag.StringVar(&cmdArgs.dbPrefix, "dbprefix", DB_FILE, "path to sqlite3 database file")
	flag.StringVar(&cmdArgs.dbSuffix, "dbsuffix", "", "sqlite3 database file suffix")
    flag.StringVar(&cmdArgs.RestServer, "a", "", "port address for rest server")

	flag.Parse()

    time.Sleep(3 * time.Second)

	return cmdArgs
}

func main() {
	log.SetLevel(log.DebugLevel)

	cmdArgs := InitCommandArgs()

	cfg := config.GetConfig(cmdArgs.ConfigName)

	log.Debugf("config: %v", cfg)

	dbConn, err := sqlx.Connect("sqlite3", cfg.DBFileName)
	if err != nil {
		log.Fatalf("failed to open database connection: %s", err.Error())
	}

	dbConn.MustExec(Schema)

	svr := &server{
		dbConn: dbConn,
		urlMu:  sync.Mutex{},

		db: &database{dbConn},

		raftServer: nil,
	}

	raftServer := raft.New(dbConn, &raft.NewStateArgs{
		ApplyLog: svr.ApplyLogs,
		SelfNode: cmdArgs.Self,
		Config:   cfg,
	})

	svr.raftServer = raftServer

	go func() {
		svr.raftServer.Run()
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("POST /{$}", svr.postHandler)
	mux.HandleFunc("GET /{id}", svr.getHandler)
	log.Println("running")

	err = http.ListenAndServe(cmdArgs.RestServer, mux)
	if err != nil {
		log.Panicf("short url server failed: %v", err)
	}

}
