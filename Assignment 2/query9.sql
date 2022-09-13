-- Example SET @v1 = '2';
CREATE TABLE query9 AS

SELECT movieid, rating 
FROM ratings
WHERE userid = @v1