CREATE TABLE mfs (
            PATH 	TEXT PRIMARY KEY UNIQUE,
            PARENT	TEXT NOT NULL,
            DIR		BOOLEAN NOT NULL,
            SIZE 	INTEGER NOT NULL,
            MODTIME TIME NOT NULL,
            MDSID INT references mds(ID),
            OWNER TEXT
);
CREATE INDEX parent_idx ON mfs (parent);
