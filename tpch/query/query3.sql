--INCLUDE 'examples/queries/tpch/schemas.sql';

CREATE TABLE LINEITEM (
  orderkey int REFERENCES ORDERS,
  partkey int REFERENCES PART,
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


-- SELECT ORDERS.orderkey, 
--        ORDERS.orderdate,
--        ORDERS.shippriority,
--        SUM(extendedprice * (1 - discount)) AS query3
-- FROM   CUSTOMER, ORDERS, LINEITEM
-- WHERE  CUSTOMER.mktsegment = 'BUILDING'
--   AND  ORDERS.custkey = CUSTOMER.custkey
--   AND  LINEITEM.orderkey = ORDERS.orderkey
--   AND  ORDERS.orderdate < DATE('1995-03-15')
--   AND  LINEITEM.shipdate > DATE('1995-03-15')
-- GROUP BY ORDERS.orderkey, ORDERS.orderdate, ORDERS.shippriority;


--SELECT LINEITEM.ORDERKEY, SUM(LINEITEM.EXTENDEDPRICE * (1 - LINEITEM.DISCOUNT)) 
--AS REVENUE, ORDERS.ORDERDATE, ORDERS.SHIPPRIORITY FROM CUSTOMER, ORDERS, LINEITEM 
--WHERE CUSTOMER.MKTSEGMENT = 'BUILDING' AND CUSTOMER.CUSTKEY = ORDERS.CUSTKEY AND 
--LINEITEM.ORDERKEY = ORDERS.ORDERKEY 
--AND ORDERS.ORDERDATE < DATE('1995-03-15') AND LINEITEM.SHIPDATE > DATE('1995-03-15') GROUP BY 
--LINEITEM.ORDERKEY, ORDERS.ORDERDATE, ORDERS.SHIPPRIORITY ORDER BY REVENUE DESC, ORDERS.ORDERDATE

SELECT
   lineitem.orderkey,
   sum(lineitem.extendedprice * (1 - lineitem.discount)) AS revenue,
   orders.orderdate,
   orders.shippriority 
FROM
   customer, orders, lineitem 
WHERE
   customer.mktsegment = 'HOUSEHOLD' 
   AND customer.custkey = orders.custkey 
   AND lineitem.orderkey = orders.orderkey 
   AND orders.orderdate < DATE('1995-03-24') 
   AND lineitem.shipdate > DATE('1995-03-24') 
GROUP BY
   lineitem.orderkey,
   orders.orderdate,
   orders.shippriority 
ORDER BY
   revenue DESC,
   orderdate LIMIT 10
