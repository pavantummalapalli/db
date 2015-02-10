CREATE TABLE PLAYERS(
  ID string, 
  FIRSTNAME string, 
  LASTNAME string, 
  FIRSTSEASON int, 
  LASTSEASON int, 
  WEIGHT int, 
  BIRTHDATE date
);

SELECT FIRSTNAME, LASTNAME, WEIGHT, BIRTHDATE 
FROM PLAYERS WHERE WEIGHT>200
