--INCLUDE 'examples/queries/tpch/schemas.sql';

CREATE TABLE LINEITEM (
  orderkey int REFERENCES ORDERS,
  partkey int REFERENCES PARTS,
  suppkey int REFERENCES SUPPLIERS,
  linenumber int,
  quantity decimal,
  extendedprice decimal,
  discount decimal,
  tax decimal,
  returnflag char(1),
  linestatus char(1),
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct char(25),
  shipmode char(10),
  comment varchar(44),
  PRIMARY KEY (orderkey, linenumber)
);

 CREATE TABLE ORDERS (
  orderkey int,
  custkey int REFERENCES CUSTOMER,
  orderstatus char(1),
  totalprice decimal,
  orderdate date,
  orderpriority char(15),
  clerk char(15),
  shippriority int,
  comment varchar(79),
  PRIMARY KEY (orderkey)
);

CREATE TABLE PART (
  partkey int,
  name varchar(55),
  mfgr char(25),
  brand char(10),
  type varchar(25),
  size int,
  container char(10),
  retailprice decimal,
  comment varchar(23),
  PRIMARY KEY (partkey)
);

CREATE TABLE CUSTOMER (
  custkey int,
  name varchar(25),
  address varchar(40),
  nationkey int REFERENCES NATION,
  phone char(15),
  acctbal decimal,
  mktsegment char(10),
  comment varchar(117),
  PRIMARY KEY (custkey)
);

CREATE TABLE SUPPLIER (
  suppkey int,
  name char(25),
  address varchar(40),
  nationkey int REFERENCES NATION,
  phone char(15),
  acctbal decimal,
  comment varchar(101),
  PRIMARY KEY (suppkey)
);

CREATE TABLE PARTSUPP (
  partkey int REFERENCES PART,
  suppkey int REFERENCES SUPPLIER,
  availqty int,
  supplycost decimal,
  comment varchar(199),
  PRIMARY KEY (partkey, suppkey)
);

CREATE TABLE NATION (
  nationkey int,
  name char(25),
  regionkey int REFERENCES REGION,
  comment varchar(152),
  PRIMARY KEY (nationkey)
);

CREATE TABLE REGION (
  regionkey int,
  name char(25),
  comment varchar(152),
  PRIMARY KEY (regionkey)
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