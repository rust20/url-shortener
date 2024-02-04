CREATE TABLE ShortURL (url_id TEXT PRIMARY KEY,
                                           url_full TEXT NOT NULL UNIQUE);

CREATE INDEX index_url_full ON ShortURL(url_full);
CREATE INDEX index_url_id ON ShortURL(url_id);

CREATE TABLE Logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    log_term INTEGER NOT NULL,
    log_idx INTEGER NOT NULL,

	op TEXT NOT NULL,
	strkey TEXT NOT NULL,
	strval TEXT NOT NULL
);

CREATE INDEX index_log_id ON Logs(log_id);
CREATE INDEX index_log_idx ON Logs(log_idx);
