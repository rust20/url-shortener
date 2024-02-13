package main

const Schema = `
PRAGMA synchronous = OFF;
PRAGMA journal_mode = MEMORY;

CREATE TABLE IF NOT EXISTS ShortURL (
    url_id TEXT PRIMARY KEY,
    url_full TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS index_url_full ON ShortURL(url_full);
CREATE INDEX IF NOT EXISTS index_url_id ON ShortURL(url_id);

CREATE TABLE IF NOT EXISTS Logs (
    log_idx INTEGER PRIMARY KEY,
    log_term INTEGER NOT NULL,

	op TEXT NOT NULL,
	strkey TEXT NOT NULL,
	strval TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS index_log_idx ON Logs(log_idx);

CREATE TABLE IF NOT EXISTS Node (
    address TEXT NOT NULL,
    rest_address TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS KeyVal (
    key TEXT NOT NULL UNIQUE,
    val TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS index_key_keyval ON KeyVal(key);

CREATE TABLE IF NOT EXISTS RaftStateful (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    term TEXT NOT NULL,
    voted_for TEXT NOT NULL
);
`
