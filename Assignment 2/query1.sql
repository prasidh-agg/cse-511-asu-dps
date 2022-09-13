CREATE TABLE query1 AS

SELECT g.name, COUNT(movieid) AS moviecount
FROM hasagenre hg
JOIN genres g
	ON hg.genreid = g.genreid
GROUP BY g.name;