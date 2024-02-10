package main

import (
	"encoding/csv"
	"net/http"

	log "github.com/sirupsen/logrus"
)

const clickbenchUrl = "https://db.in.tum.de/teaching/ws2324/clouddataprocessing/data/filelist.csv"

const limitdataset = 3

func main() {

	resDataSet, err := http.Get(clickbenchUrl)
	if err != nil {
        log.Fatal("get dataset fail:", err)
	}

	dataSet, err := csv.NewReader(resDataSet.Body).ReadAll()
	if err != nil {
		log.Fatal("parse csv dataset fail", err)
	}

    for _, data := range dataSet[:limitdataset] {
        log.Info(data)

		res, err := http.Get(data[0])
		if err != nil {
            log.Fatal("get partial dataset fail:", err)
		}

		resReader := csv.NewReader(res.Body)
        resReader.Comma = '\t'
        dataContent, err := resReader.ReadAll()
		if err != nil {
			log.Fatal("parse csv partial dataset fail", err)
		}

		for _, row := range dataContent {
			log.Info(row)
		}
	}

	// todo:
	// fetch csv
	// read csv
	// construct payload
	// send payload
}
