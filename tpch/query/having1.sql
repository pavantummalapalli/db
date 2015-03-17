CREATE TABLE R (A int, B int);
select A,B,SUM(R.B) from R GROUP BY A HAVING COUNT(R.B) > 1;

//TODO check for such Queries later..
