CREATE TABLE R (A int, B int);
CREATE TABLE S (B int, C int);


SELECT R.B, SUM(A * C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B, C ORDER BY A ASC,R.B DESC;

SELECT Z.A, SUM(C) FROM R as Z, S WHERE R.B = S.B GROUP BY A;

SELECT A, SUM(C) FROM R, S WHERE R.B = S.B GROUP BY A;
SELECT A, R.B, SUM(C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B;
SELECT R.B, SUM(A * C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B, C;
