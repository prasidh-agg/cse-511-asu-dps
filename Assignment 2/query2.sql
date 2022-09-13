CREATE TABLE query2 AS 

SELECT g.name, CAST(AVG(rating) AS DECIMAL(15,10)) AS rating
FROM ratings r
JOIN hasagenre hg
	ON r.movieid = hg.movieid
JOIN genres g
	ON hg.genreid = g.genreid
GROUP BY g.name;