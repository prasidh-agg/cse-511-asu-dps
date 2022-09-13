CREATE TABLE query3 AS
SELECT m.title, COUNT(rating) AS countofratings
FROM movies m
JOIN ratings r
 	ON m.movieid = r.movieid
GROUP BY m.title
HAVING COUNT(*) >= 10;