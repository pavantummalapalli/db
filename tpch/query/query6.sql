--INCLUDE 'examples/queries/tpch/schemas.sql';
CREATE TABLE LINEITEM (
        orderkey       INT,
        partkey        INT,
        suppkey        INT,
        linenumber     INT,
        quantity       DOUBLE,
        extendedprice  DOUBLE,
        discount       DOUBLE,
        tax            DOUBLE,
        returnflag     STRING,
        linestatus     STRING,
        shipdate       DATE,
        commitdate     DATE,
        receiptdate    DATE,
        shipinstruct   STRING,
        shipmode       STRING,
        comment        STRING
);

SELECT SUM(l.extendedprice*l.discount) AS revenue
FROM   lineitem l
WHERE  l.shipdate >= DATE('1994-01-01')
  AND  l.shipdate < DATE('1995-01-01')
  AND  (l.discount BETWEEN (0.06 - 0.01) AND (0.06 + 0.01)) 
  AND  l.quantity < 24;
