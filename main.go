package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"net/http"
	netURL "net/url"
	"sync"

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

const ID_LENGHT = 8
const MAX_URL_LENGTH = 20000
const DB_FILE = "store.db"
const CheckInvalidURL = false

type serverOp int

const (
	OpInsert serverOp = iota
	OpDelete
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

	// TODO: redirect if not leader

	if !s.raftServer.IsLeader() {
		target := fmt.Sprintf("http://%s%s", s.raftServer.GetLeaderAddr(), req.URL.Path)
		http.Redirect(w, req, target, http.StatusPermanentRedirect)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if len(req.URL.Path) < 1 {
		http.Error(w, ErrInvalidRequest.Error(), http.StatusBadRequest)
		return
	}

	url_id := req.PathValue("id")

	url, err := s.db.GetShortURL(url_id)
	if err != nil {
        log.Errorf("rest server getshorturl 3: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	http.Redirect(w, req, url, http.StatusSeeOther)
	return
}

func (s *server) postHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("[+] POST | path: %s \n", req.URL.Path)

	if !s.raftServer.IsLeader() {
		target := fmt.Sprintf("http://%s%s", s.raftServer.GetLeaderAddr(), req.URL.Path)
		http.Redirect(w, req, target, http.StatusPermanentRedirect)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	req.ParseForm()
	urls, ok := req.PostForm["url"]
	if !ok || len(urls) == 0 {
		log.Errorf("invalid request")

        body, _ := io.ReadAll(req.Body)
        log.Error("data raw: ", body)
		http.Error(w, ErrInvalidRequest.Error(), http.StatusBadRequest)
		return
	}
	url := urls[0]

	if CheckInvalidURL && !validateURL(url) {
		log.Errorln("invalid url")
		http.Error(w, ErrInvalidURL.Error(), http.StatusBadRequest)
		return
	}
	res, err := s.CreateShortURL(url)
	if err != nil {
		log.Errorln("failed to create url")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	storedURLId, err := s.db.GetShortURL(res)

	if err != nil && err != sql.ErrNoRows {
        log.Errorf("rest server getshorturl 1: %s", err.Error())
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

	// TODO: insert log

	// err = s.raftServer.BroadcastEntries()
	// if err != nil {
	// 	http.Error(w, err.Error(), http.StatusBadRequest)
	// 	return
	// }

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

func (s *server) CreateShortURL(urlFull string) (string, error) {
	if len(urlFull) > MAX_URL_LENGTH {
		return "", ErrURLTooLong
	}

	urlId, err := s.db.GetShortURL(urlFull)
	if err != nil && err != sql.ErrNoRows {
        log.Errorf("rest server getshorturl 2: %s", err.Error())
		return "", nil
	}


    log.Infof("url result is %s", urlId)

	if urlId == "" && err == sql.ErrNoRows {
		return urlId, nil
	}

    log.Info("creating url instead...")

	urlId = s.HashURL(urlFull)
	err = s.db.InsertShortURL(urlId, urlFull)
	if err != nil {
        log.Errorf("rest server insertshorturl: %s", err.Error())
		return "", err
	}

	err = s.raftServer.InsertLog(raft.Log{
		Op:    int(OpInsert),
		Key:   urlId,
		Value: urlFull,
	})
	if err != nil {
        log.Errorf("rest server raft insertlog: %s", err.Error())
		return "", err
	}

	return urlId, nil
}

func (s *server) HashURL(url string) string {
	hasher := md5.New()
	hasher.Write([]byte(url))

	encoded := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
	return encoded[:ID_LENGHT]
}

func (s *server) ApplyLogs(self *raft.State) {
	logs, err := self.GetLogs(self.LastApplied, self.CommitIndex-self.LastApplied)
	if err != nil {
		log.Panicf("raft: apply log fail, unable to get logs: %s", err.Error())
	}

	for _, dataLog := range logs {
		url_id := dataLog.Key
		url_full := dataLog.Value
		if dataLog.Op == int(OpInsert) {
			err := s.db.InsertShortURL(url_id, url_full)
			if err != nil {
				log.Panicf("server error inserting: %s", err.Error())
			}

		} else if dataLog.Op == int(OpDelete) {
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

	Self       int
	RestServer string
	dbFileName string
	dbSuffix   string
}

func InitCommandArgs() CommandArgs {
	cmdArgs := CommandArgs{}

	flag.IntVar(&cmdArgs.Self, "self-id", 0, "index of current id from list of nodes defined in config")

	flag.StringVar(&cmdArgs.ConfigName, "c", "config.yaml", "config file name")
	flag.StringVar(&cmdArgs.dbFileName, "dbprefix", DB_FILE, "path to sqlite3 database file")
	flag.StringVar(&cmdArgs.RestServer, "a", "", "port address for rest server")

	flag.Parse()

	return cmdArgs
}

func main() {
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.TextFormatter{
		ForceColors: true,
		ForceQuote:  true,
	})

	cmdArgs := InitCommandArgs()

	cfg := config.GetConfig(cmdArgs.ConfigName)

	dbConn, err := sqlx.Connect("sqlite3", cmdArgs.dbFileName)
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

		Config: cfg,
	})

	svr.raftServer = raftServer

	go func() {
        log.Infof("running raft on %s", cfg.Nodes[cmdArgs.Self])
		svr.raftServer.Run()
	}()

	mux := http.NewServeMux()


	mux.HandleFunc("POST /", svr.postHandler)
	mux.HandleFunc("GET /{id}", svr.getHandler)
	log.Infof("running server on %s", cmdArgs.RestServer)

	err = http.ListenAndServe(cmdArgs.RestServer, logRequest(mux))
	if err != nil {
		log.Panicf("short url server failed: %v", err)
	}

}

func logRequest(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s\n", r.RemoteAddr, r.Method, r.URL)
		handler.ServeHTTP(w, r)
	})
}
