package db

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"gitlab.db.in.tum.de/rust20/url-shortener/raft"
)

type database struct {
	db *sql.DB
}

func (d *database) InsertNode(node raft.Node) error {
	return nil
}
func (d *database) ListNodes() ([]raft.Node, error) {
	return nil, nil
}
func (d *database) ListLogs() ([]raft.Log, error) {
	return nil, nil
}

func (d *database) InsertShortURL(url_id string, url string) error {
	_, err := d.db.Exec("insert into ShortURL values(?, ?)", url_id, url)
	if err != nil {
		return err
	}

	return nil
}
func (d *database) GetShortURL(url_id string) (string, error) {
    rows, err := d.db.Query("select url_full from ShortURL where url_id = ? limit 1", url_id)
    if err != nil {
        return "", err
    }
    defer rows.Close()

    res := ""
    for rows.Next() {
        err = rows.Scan(&res)
        if err != nil {
            return "", err
        }
    }

    return res, nil
}

