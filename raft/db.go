package raft

import (
	"fmt"

	sql "github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type database struct {
	db *sql.DB
}

func (d *database) InsertNode(node Node) error {
	_, err := d.db.Exec("INSERT INTO Node (address) values (?)",
		node.Addr,
	)
	if err != nil {
		return err
	}

	return nil
}

func (d *database) ListNodes() ([]Node, error) {
	return nil, nil
}

func (d *database) InsertLogs(logs []Log) error {
	tx := d.db.MustBegin()

	count := 0
	err := tx.Get(
		&count,
		"SELECT count(*) FROM Logs",
	)
	if err != nil {
		return err
	}

	nextId := count

	query := "" +
		"INSERT INTO Logs(log_idx, log_term, op, strkey, strval) " +
		"values(:idx, :term, :opr, :key, :val)"

	for _, log := range logs {
		log.Idx = nextId
		nextId += 1
		result, err := tx.NamedExec(query,
			map[string]interface{}{
				"idx":  log.Idx,
				"term": log.Term,
				"opr":  log.Op,
				"key":  log.Key,
				"val":  log.Value,
			})

		if err != nil {
			return err
		}

		lastId, err := result.LastInsertId()
		if err != nil {
			return err
		}

		if int(lastId) != count {
			err = tx.Rollback()
			if err != nil {
				return err
			}
			return fmt.Errorf("mismatch id: expecting %d, got %d", count, lastId)
		}

		count += 1
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (d *database) LogsLength() (int, error) {
	result := 0
	err := d.db.Get(
		&result,
		"SELECT count(*) FROM Logs",
	)
	return result, err
}

func (d *database) ListLogs(indexStart int, count int) ([]Log, error) {
	result := []Log{}
	err := d.db.Select(
		&result,
		"SELECT * FROM Logs WHERE log_idx >= ? LIMIT ?",
		indexStart,
		count,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *database) GetLogByIdx(idx int) (*Log, error) {
    if idx < 0 {
        return nil, nil
    }
	res, err := d.ListLogs(idx, 1)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	return &res[0], nil
}

func (d *database) GetLastLog() (*Log, error) {
	result := &Log{}
	err := d.db.Get(
		result,
		"SELECT * FROM Logs ORDER BY log_idx DESC LIMIT 1",
	)
	if err != nil {
		return nil, err
	}
	if result.Idx == 0 {
		return nil, nil
	}
	return result, nil
}

func (d *database) DeleteLogs(idx int) error {
	_, err := d.db.Exec("DELETE FROM Logs WHERE log_idx >= ?",
		idx)
	if err != nil {
		return err
	}

	return nil
}

func (d *database) GetNodes() ([]Node, error) {
	nodes := []Node{}
	err := d.db.Select(&nodes, "SELECT * FROM Node")
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

func (d *database) SetKey(key string, val interface{}) error {
	_, err := d.db.NamedExec("INSERT or replace INTO KeyVal (key, val) values (:key, :val)",
		map[string]interface{}{
			"key": key,
			"val": val,
		})
	return err
}

func (d *database) GetValString(key string) (string, error) {
	value := ""
	err := d.db.Get(&value, "SELECT val FROM KeyVal where key = ? limit 1", key)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (d *database) GetValInt(key string) (int, error) {
	value := 0
	err := d.db.Get(&value, "SELECT val FROM KeyVal where key = ? limit 1", key)
	if err != nil {
		return 0, err
	}
	return value, nil
}
