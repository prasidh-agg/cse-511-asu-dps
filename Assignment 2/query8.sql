CREATE TABLE query8 AS
SELECT CAST(AVG(ratings.rating) AS DECIMAL(15,10)) AS average
FROM ratings
WHERE movieid IN (SELECT hg.movieid
				  FROM hasagenre hg  
				  NATURAL JOIN genres g
				  GROUP BY hg.movieid
				  HAVING COUNT(CASE WHEN g.name = 'Comedy' THEN 1 END) = 0
				  AND COUNT(CASE WHEN g.name = 'Romance' THEN 1 END) = 1);