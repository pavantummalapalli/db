--INCLUDE 'examples/queries/tpch/schemas.sql';

CREATE TABLE LINEITEM (
       	orderkey       INT,
        partkey        INT,
        suppkey        INT,
        linenumber     INT,
        quantity       DECIMAL,
        extendedprice  DECIMAL,
        discount       DECIMAL,
        tax            DECIMAL,
        returnflag     CHAR(1),
        linestatus     CHAR(1),
        shipdate       DATE,
        commitdate     DATE,
        receiptdate    DATE,
        shipinstruct   CHAR(25),
        shipmode       CHAR(10),
        comment        VARCHAR(44)
);

-- SELECT returnflag, linestatus, 
--   SUM(quantity) AS sum_qty,
--   SUM(extendedprice) AS sum_base_price,
--   SUM(extendedprice * (1-discount)) AS sum_disc_price,
--   SUM(extendedprice * (1-discount)*(1+tax)) AS sum_charge,
--   AVG(quantity) AS avg_qty,
--   AVG(extendedprice) AS avg_price,
--   AVG(discount) AS avg_disc,
--   COUNT(*) AS count_order
-- FROM lineitem WHERE shipdate <= DATE('1997-09-01')
-- GROUP BY returnflag, linestatus; 

SELECT returnflag,
       linestatus,
       sum(quantity) AS sum_qty,
       sum(extendedprice) AS sum_base_price,
       sum(extendedprice * (1 - discount)) AS sum_disc_price,
       sum(extendedprice * (1 - discount) * (1 + tax)) AS sum_charge,
       avg(quantity) AS avg_qty,
       avg(extendedprice) AS avg_price,
       avg(discount) AS avg_disc,
       count(*) AS count_order
FROM lineitem
WHERE shipdate <= DATE('1998-09-01')
GROUP BY returnflag,
         linestatus
ORDER BY returnflag,
         linestatus;