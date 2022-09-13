CREATE TABLE query4 AS
SELECT m.movieid, m.title
FROM movies m
JOIN hasagenre hg
	ON m.movieid = hg.movieid
JOIN genres g	
	ON hg.genreid = g.genreid
WHERE g.name = 'Comedy';
