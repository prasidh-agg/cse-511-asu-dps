-- Check for duplicate tables

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS taginfo;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS hasagenre;

-- Create 7 tables required for the database
-- Just one primary key/column required to identify a unique user
CREATE TABLE users (
	userid INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- Just one primary key/column required to identify a unique movie
CREATE TABLE movies (
	movieid INTEGER PRIMARY KEY,
    title TEXT NOT NULL
);

-- Just one primary key/column required to identify a unique tag  
CREATE TABLE taginfo (
	tagid INTEGER PRIMARY KEY,
    content TEXT NOT NULL
);

-- Just one primary key/column required to identify a unique genre  
CREATE TABLE genres (
	genreid INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
    
-- Combination of userid and movieid used as a composite key to
-- identify a unique rating  
CREATE TABLE ratings (
	userid INTEGER NOT NULL, 
    movieid INTEGER NOT NULL,
    rating NUMERIC(2,1) CHECK(rating >= 0.0 and rating <= 5.0) NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (userid, movieid),
    FOREIGN KEY (userid) REFERENCES users(userid),
    FOREIGN KEY (movieid) REFERENCES movies(movieid)
);

-- Combination of userid and movieid and tagid used as a composite key to
-- identify a unique tag
CREATE TABLE tags(
	userid INTEGER NOT NULL,
    movieid INTEGER NOT NULL,
    tagid INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY (userid, movieid, tagid),
    FOREIGN KEY(userid) REFERENCES users(userid),
    FOREIGN KEY(movieid) REFERENCES movies(movieid),
    FOREIGN KEY(tagid) REFERENCES taginfo(tagid)
);

-- Combination of movieid and genre used as a composite key to
-- identify a unique hasagenre entry
CREATE TABLE hasagenre (
    movieid INTEGER,
    genreid INTEGER,
    PRIMARY KEY (movieid , genreid),
    FOREIGN KEY (movieid)
        REFERENCES movies (movieid),
    FOREIGN KEY (genreid)
        REFERENCES genres (genreid)
);

-- Uncomment below to load test data into tables. 
-- Also change the absolute path

-- LOAD DATA LOCAL INFILE '/Users/titanium/Desktop/CSE 511 DPS/Assignment 1/users.dat' INTO TABLE users
-- FIELDS TERMINATED BY '%';

-- LOAD DATA LOCAL INFILE '/Users/titanium/Desktop/CSE 511 DPS/Assignment 1/movies.dat' INTO TABLE movies
-- FIELDS TERMINATED BY '%';

-- LOAD DATA LOCAL INFILE '/Users/titanium/Desktop/CSE 511 DPS/Assignment 1/taginfo.dat' INTO TABLE taginfo
-- FIELDS TERMINATED BY '%';

-- LOAD DATA LOCAL INFILE '/Users/titanium/Desktop/CSE 511 DPS/Assignment 1/genres.dat' INTO TABLE genres
-- FIELDS TERMINATED BY '%';

-- LOAD DATA LOCAL INFILE '/Users/titanium/Desktop/CSE 511 DPS/Assignment 1/ratings.dat' INTO TABLE ratings
-- FIELDS TERMINATED BY '%';

-- LOAD DATA LOCAL INFILE '/Users/titanium/Desktop/CSE 511 DPS/Assignment 1/tags.dat' INTO TABLE tags
-- FIELDS TERMINATED BY '%';

-- LOAD DATA LOCAL INFILE '/Users/titanium/Desktop/CSE 511 DPS/Assignment 1/hasagenre.dat' INTO TABLE hasagenre
-- FIELDS TERMINATED BY '%';
    