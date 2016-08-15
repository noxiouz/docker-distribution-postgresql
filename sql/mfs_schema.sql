CREATE TABLE mfs (
            PATH 	TEXT PRIMARY KEY UNIQUE,
            PARENT	TEXT NOT NULL,
            DIR		BOOLEAN NOT NULL,
            SIZE 	BIGINT NOT NULL,
            MODTIME TIME NOT NULL,
            KEY     TEXT,
            OWNER   TEXT
);
CREATE INDEX parent_idx ON mfs (parent);
