CREATE TABLE PLAYERS(
  ID string, 
  FIRSTNAME string, 
  LASTNAME string, 
  FIRSTSEASON int, 
  LASTSEASON int, 
  WEIGHT int, 
  BIRTHDATE date
);

--SELECT FIRSTNAME, LASTNAME, WEIGHT, BIRTHDATE 
--FROM PLAYERS WHERE WEIGHT>200;

--SELECT a FROM b
--UNION 
--SELECT c FROM d;

SELECT DISTINCT ml.movieTitle, ml.movieYear
FROM StarsIn ml
WHERE ml.movieYear - 40 <= (
	SELECT AVG(birthdate)
	FROM StarsIn m2, MovieStar s
	WHERE m2.starName = s .name AND
	      ml.movieTitle = m2.movieTitle AND
	      ml.movieYear = m2.movieYear
);

SELECT R.A, T.D FROM B,A,C WHERE A.B1 = B.B1 AND (C.B2 < B.B1);

--SELECT R.A, T.D FROM (B JOIN R)  WHERE R.B = S.B AND (T.C < S.C);
