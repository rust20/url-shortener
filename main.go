package main

import (
	"fmt"
	"log"
	"net/http"
	netURL "net/url"

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

type server struct {
	urlToId  map[string]string
	idToUrl  map[string]string
	idLength int32
}

type ShortURLResponse struct {
	Status   int32  `json:"status"`
	ShortURL string `json:"short_url,omitempty"`
	URL      string `json:"URL,omitempty"`
	Message  string `json:"message"`
}

func (s *server) handler(w http.ResponseWriter, req *http.Request) {
	log.Printf("[+] path: %s | meth: %s\n", req.URL.Path, req.Method)
	w.Header().Set("Content-Type", "application/json")

	if req.Method == "POST" {
		req.ParseForm()
		urls, ok := req.PostForm["url"]
		if !ok || len(urls) == 0 {
			log.Println("invalid request")
			handleBadRequestError(w, ErrInvalidRequest.Error())
			return
		}
		url := urls[0]
		if !validateURL(url) {
			log.Println("invalid url")
			handleBadRequestError(w, ErrInvalidURL.Error())
			return
		}
		res, err := s.CreateShortURL(url)
		if err != nil {
			handleBadRequestError(w, err.Error())
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
			handleInternalServerError(w, err.Error())
		}
		w.Write(responseJson)
		return

	} else if req.Method == "GET" {
		if len(req.URL.Path) < 1 {
			handleBadRequestError(w, ErrInvalidRequest.Error())
			return
		}

		url_id := req.URL.Path[1:]
		url, err := s.GetShortURL(url_id)
		if err != nil {
			handleBadRequestError(w, err.Error())
			return
		}

		http.Redirect(w, req, url, http.StatusSeeOther)

		// response := ShortURLResponse{
		// 	Status:   http.StatusAccepted,
		// 	URL: url,
		// 	Message:  "success",
		// }
		//
		//       responseJson, err := json.Marshal(response)
		//       if err != nil {
		//           handleInternalServerError(w, err.Error())
		//       }
		//       w.Write(responseJson)

		return
	}
}

func handleInternalServerError(w http.ResponseWriter, message string) {
	handleError(w, message, http.StatusInternalServerError)
}

func handleBadRequestError(w http.ResponseWriter, message string) {
	handleError(w, message, http.StatusBadRequest)
}

func handleError(w http.ResponseWriter, message string, status int) {
	w.WriteHeader(status)
	resp := make(map[string]string)
	resp["message"] = message
	responseJson, err := json.Marshal(resp)
	if err != nil {
		log.Fatalf("Error happened in JSON marshal. Err: %s", err)
	}
	w.Write(responseJson)
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
		urlId = newId
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

func validateURL(url string) bool {
	_, err := netURL.ParseRequestURI(url)
	return err == nil
}

func main() {
	svr := &server{
		map[string]string{},
		map[string]string{},
		ID_LENGHT,
	}

	http.HandleFunc("/", svr.handler)
	log.Println("running")

	http.ListenAndServe(":8080", nil)

}
