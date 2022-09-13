CREATE TABLE query5 AS

SELECT title, CAST(AVG(rating) AS DECIMAL(15,10)) AS average
FROM movies m
JOIN ratings r
	ON m.movieid = r.movieid
GROUP BY title;