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

CREATE TABLE ORDERS (
        orderkey       INT,
        custkey        INT,
        orderstatus    STRING,
        totalprice     DOUBLE,
        orderdate      DATE,
        orderpriority  STRING,
        clerk          STRING,
        shippriority   INT,
        comment        STRING
);

-- SELECT l.shipmode, 
--        SUM(CASE WHEN o.orderpriority IN LIST ('1-URGENT', '2-HIGH')
--                 THEN 1 ELSE 0 END) AS high_line_count,
--        SUM(CASE WHEN o.orderpriority NOT IN LIST ('1-URGENT', '2-HIGH')
--                 THEN 1 ELSE 0 END) AS low_line_count
-- FROM   orders o, lineitem l
-- WHERE  o.orderkey = l.orderkey
--   AND  (l.shipmode IN LIST ('MAIL', 'SHIP'))
--   AND  l.commitdate < l.receiptdate
--   AND  l.shipdate < l.commitdate
--   --AND  l.receiptdate >= DATE('1994-01-01')
--   --AND  l.receiptdate < DATE('1995-01-01')
-- GROUP BY l.shipmode;



--SELECT lineitem.shipmode, sum(CASE WHEN orders.orderpriority = '1-URGENT' 
--OR orders.orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, 
--sum(CASE WHEN orders.orderpriority <> '1-URGENT' AND orders.orderpriority <> '2-HIGH' 
--THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE 
--orders.orderkey = lineitem.orderkey AND (lineitem.shipmode = 'AIR' OR 
--lineitem.shipmode = 'MAIL' OR lineitem.shipmode = 'TRUCK' OR lineitem.shipmode = 'SHIP') 
--AND lineitem.commitdate < lineitem.receiptdate AND lineitem.shipdate < lineitem.commitdate 
--AND lineitem.receiptdate >= date('1995-03-05') AND lineitem.receiptdate < date('1996-03-05') 
--GROUP BY lineitem.shipmode ORDER BY lineitem.shipmode

SELECT lineitem.shipmode,
       sum(CASE WHEN orders.orderpriority = '1-URGENT'
           OR orders.orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
       sum(CASE WHEN orders.orderpriority <> '1-URGENT'
           AND orders.orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
FROM lineitem,
     orders
WHERE orders.orderkey = lineitem.orderkey
  AND (lineitem.shipmode = 'AIR'
       OR lineitem.shipmode = 'MAIL')
  AND lineitem.commitdate < lineitem.receiptdate
  AND lineitem.shipdate < lineitem.commitdate
  AND lineitem.receiptdate >= date('1994-01-01')
  AND lineitem.receiptdate < date('1995-01-01')
GROUP BY lineitem.shipmode
ORDER BY shipmode