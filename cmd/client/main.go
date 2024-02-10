package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"encoding/csv"

	log "github.com/sirupsen/logrus"
)

const clickbenchUrl = "https://db.in.tum.de/teaching/ws2324/clouddataprocessing/data/filelist.csv"
const limitdataset = 3

type client struct {
	host string
	port string
}
type ShortURLResponse struct {
	Status   int32  `json:"status"`
	ShortURL string `json:"short_url,omitempty"`
	URL      string `json:"URL,omitempty"`
	Message  string `json:"message"`
}

func (c *client) CreateURL(url_data string) (string, error) {
	// TODO: impl retry mechanism

	form := url.Values{}
	form.Add("url", url_data)

	targetURL := fmt.Sprintf("http://%s:%s%s", c.host, c.port, "/")
	req, err := http.NewRequest(http.MethodPost, targetURL, strings.NewReader(form.Encode()))

	if err != nil {
		return "", err
	}

	// req.PostForm = form
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	reqResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}

	if reqResp.StatusCode != http.StatusOK {

        body, err := io.ReadAll(reqResp.Body)
        if err != nil {
            log.Fatal(err)
        }

        log.Error("status", reqResp.Status, "content:", string(body))
        return "", fmt.Errorf("request failed")
	}

	resp := &ShortURLResponse{}
	err = json.NewDecoder(reqResp.Body).Decode(resp)
	if err != nil {
		return "", err
	}

	return resp.ShortURL, nil
}

func try_single() {
	c := client{
		host: "localhost",
		port: "8080",
	}

	url_data := "http:%2F%2Fwwww.diary.ru/yandsearch?type=topics&lang=ru&cE=true&uA=Mozilnik.ru%2Fslovarenok.ru/GameMain.aspx&ei=LuzTUdyMAl2tPbUfb8EeS34ASWs4GADQ&usg=AFQjCNERpbuvboma-skaDw&sig=e893e60d43a06fadaachki/default8909644&source=web&cd=7&ved=0CEEQFjAI&url=http://yandsearch&input_age2=&input_vip=&int[60][2021&redir=1&limited 4 black.ru/name=yandex.ru/sell/204254&ELEMENT&room=1&d1=01.08.2013&text=смотреть онлайн  берем дженер&dated/vamp/LaRedouters=0&po_yers=2000/currency=RUR/heating=1/page2/?er=2&target=search/price_do=30000&curre/list.html?act=Longitude=600086105/Adobe-gorod/page12/page5/?elmt=137309gasHIpq_TygcJAtIp-JFwwnYCIDg&usg=AFQjCNG9M0QsxqqdKXc28T5____GN1O1Kg==&y1=2012 серии подборщик/hasimages=1/feerie.com.ua/search&evL8b-gh_masljan-paltor=&o=0&wi=1280&u_tz=0&auto.ria.ua/?target=search?q=пантены в лав и ижевской животными&t=web&cd=3&car=0&auto.ria.ua/search?clid=137062126969077806]=Y&SelectedRegion/vacuumewgAEAEAcLa4ATD8IDYBA&usg=AFQjCNHu347logout&lr=172&text=народный район страция потолока на эстоны с фото&rid=213&text=AdeletedAuto=oldAuto=oldAutos&marka=58&dep_id=9403&lr=2013-07-18&num=11&category=RUR/rebtLong=0/pageTypeSearch_user%2F46537597&lr=54&text=мчс&l=&sort=PRICE][from]=&int[21633961320/?item_no=39750&sale/viewforum/MsgList.php?g=1280&state/component/search.xml?from]=&int[9][from=yandex.ru/shosseTypeSearch/?tab=&page6/#overinki-i-net.ru/imgres?img=arminatsja-doma.ru/mileonbet-krasnodar.irr.ru/index.ru/search?clid=1372868][from]=46510&lr=21000007.msg68364%26oe%3D30000&currency=RUR/sort=newly&sle=4&streal-estate_id=0&input_who1=2&input_who1=2&input_sponsor=&o=3000.ru/chelov.ru/showall=от 5000f.html?page=2&q=прода&oq=перевод&clid=440724-81 скачать чук (непрохождения  свария боротки смотреть фильм складывать газин&newFlat=0&newwindows-1251&lr=1423920/trashbox.ru/otdam-darom/pink/frontakty_zadness/businskaia-moda-zhienskaia"
    log.Info("data length:", len(url_data))

	res, err := c.CreateURL(url_data)
	if err != nil {
		log.Fatalf("request failed: %s", err)
	}

	log.Infof("url: %s | result: %s", url_data, res)
}

func try_many() {
	c := client{
		host: "localhost",
		port: "8080",
	}

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

        total := len(dataContent)
		for i, row := range dataContent {
			url_data := row[1]

			res, err := c.CreateURL(url_data)
			if err != nil {
				log.Fatalf("request failed: %s", err)
			}

            if i % 1000 == 0 {
                log.Infof("prog: %v/%v", i, total)
            }

            log.Debugf("url: %s | result: %s", url_data, res)
		}
	}

	// todo:
	// fetch csv
	// read csv
	// construct payload
	// send payload
}

func main() {
    log.SetLevel(log.InfoLevel)
	// try_single()
	try_many()
}

