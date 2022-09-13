CREATE TABLE query7 AS
SELECT CAST(AVG(ratings.rating) AS DECIMAL(15,10)) AS average
FROM ratings
INNER JOIN (SELECT hasagenre.movieid
            FROM hasagenre  
            NATURAL JOIN genres
            WHERE genres.name IN ('Comedy', 'Romance')
            GROUP BY hasagenre.movieid
            HAVING COUNT(DISTINCT genres.name) = 2
)m ON ratings.movieid = m.movieid;