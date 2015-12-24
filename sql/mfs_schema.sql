CREATE TABLE mfs (
            PATH 	VARCHAR(256) PRIMARY KEY UNIQUE,
            PARENT	VARCHAR(256) NOT NULL,
            DIR		BOOLEAN NOT NULL,
            SIZE 	INTEGER NOT NULL,
            MODTIME TIME NOT NULL,
            MDSID INT references mds(ID)
);
CREATE INDEX parent_idx ON mfs (parent);
