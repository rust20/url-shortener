package main

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type database struct {
	db *sqlx.DB
}

func (d *database) InsertShortURL(url_id string, url string) error {
	_, err := d.db.NamedExec("INSERT INTO ShortURL (url_id, url_full) VALUES (:url_id, :url_full)", map[string]interface{}{
		"url_id":   url_id,
		"url_full": url,
	})
	if err != nil {
		return err
	}

	return nil
}
func (d *database) DeleteShortURL(url_id string) error {
	_, err := d.db.NamedExec("DELETE FROM ShortURL WHERE url_id = $1", url_id)
	if err != nil {
		return err
	}

	return nil
}
func (d *database) GetShortURL(url_id string) (string, error) {

	result := []string{}
	err := d.db.Select(&result, "select url_full from ShortURL where url_id = $1 limit 1", url_id)
	if err != nil {
		return "", err
	}

	if len(result) < 1 {
		return "", ErrNotFound
	}

	return result[0], nil
}
