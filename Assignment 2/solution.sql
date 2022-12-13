-- Q1
-- Write a SQL query to return the total number of movies for each genre

CREATE TABLE query1 AS
SELECT g.name, COUNT(movieid) AS moviecount
FROM hasagenre hg
JOIN genres g
	ON hg.genreid = g.genreid
GROUP BY g.name;

-- Q2
-- Write a SQL query to return the average rating per genre

CREATE TABLE query2 AS 
SELECT g.name, CAST(AVG(rating) AS DECIMAL(15,10)) AS rating
FROM ratings r
JOIN hasagenre hg
	ON r.movieid = hg.movieid
JOIN genres g
	ON hg.genreid = g.genreid
GROUP BY g.name;

-- Q3
-- Write a SQL query to return the movies which have at least 10 ratings.

CREATE TABLE query3 AS
SELECT m.title, COUNT(rating) AS countofratings
FROM movies m
JOIN ratings r
 	ON m.movieid = r.movieid
GROUP BY m.title
HAVING COUNT(*) >= 10;

-- Q4
-- Write a SQL query to return all “Comedy” movies, including movieid and title.

CREATE TABLE query4 AS
SELECT m.movieid, m.title
FROM movies m
JOIN hasagenre hg
	ON m.movieid = hg.movieid
JOIN genres g	
	ON hg.genreid = g.genreid
WHERE g.name = 'Comedy';

-- Q5
-- Write a SQL query to return the average rating per movie

CREATE TABLE query5 AS
SELECT title, CAST(AVG(rating) AS DECIMAL(15,10)) AS average
FROM movies m
JOIN ratings r
	ON m.movieid = r.movieid
GROUP BY title;

-- Q6
-- Write a SQL query to return the average rating for all “Comedy” movies.

CREATE TABLE query6 AS 
SELECT CAST(AVG(rating) AS DECIMAL(15,10)) AS average
FROM ratings
NATURAL JOIN hasagenre hg
NATURAL JOIN genres g
WHERE g.name = 'Comedy';

-- Q7
-- Write a SQL query to return the average rating for all movies and each of these movies is both "Comedy" and "Romance"

CREATE TABLE query7 AS
SELECT CAST(AVG(ratings.rating) AS DECIMAL(15,10)) AS average
FROM ratings
INNER JOIN (SELECT hg.movieid
            FROM hasagenre hg
            NATURAL JOIN genres g
            WHERE g.name IN ('Comedy', 'Romance')
            GROUP BY hg.movieid
            HAVING COUNT(DISTINCT g.name) = 2
)result ON ratings.movieid = result.movieid;

-- Q8
-- Write a SQL query to return the average rating for all movies and each of these movies is "Romance" but not "Comedy"

CREATE TABLE query8 AS
SELECT CAST(AVG(ratings.rating) AS DECIMAL(15,10)) AS average
FROM ratings
WHERE movieid IN (SELECT hg.movieid
				  FROM hasagenre hg  
				  NATURAL JOIN genres g
				  GROUP BY hg.movieid
				  HAVING COUNT(CASE 
									WHEN g.name = 'Comedy' THEN 1 
                                    END) = 0
				  AND COUNT(CASE 
									WHEN g.name = 'Romance' THEN 1 
                                    END) = 1);


-- Q9
-- Find all movies that are rated by a user such that the userId is equal to v1

-- Example SET @v1 = '2';
CREATE TABLE query9 AS
SELECT movieid, rating 
FROM ratings
WHERE userid = @v1