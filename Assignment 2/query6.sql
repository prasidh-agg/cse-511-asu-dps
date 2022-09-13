CREATE TABLE query6 AS 
SELECT CAST(AVG(rating) AS DECIMAL(15,10)) AS average
FROM ratings
NATURAL JOIN hasagenre hg
NATURAL JOIN genres g
WHERE g.name = 'Comedy';
