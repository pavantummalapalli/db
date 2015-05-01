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

--SELECT  c.custkey, c.name, 
--        c.acctbal,
--        n.name,
--        c.address,
--        c.phone,
--        c.comment,
--        SUM(l.extendedprice * (1 - l.discount)) AS revenue
--FROM    customer c, orders o, lineitem l, nation n
--WHERE   c.custkey = o.custkey
--  AND   l.orderkey = o.orderkey
--  AND   o.orderdate >= DATE('1993-10-01')
--  AND   o.orderdate < DATE('1994-01-01')
--  AND   l.returnflag = 'R'
--  AND   c.nationkey = n.nationkey
--GROUP BY c.custkey, c.name, c.acctbal, c.phone, n.name, c.address, c.comment


SELECT customer.custkey,
       sum(lineitem.extendedprice * (1 - lineitem.discount)) AS revenue,
       customer.acctbal,
       nation.name,
       customer.address,
       customer.phone,
       customer.comment
FROM customer,
     orders,
     lineitem,
     nation
WHERE customer.custkey = orders.custkey
  AND lineitem.orderkey = orders.orderkey
  AND orders.orderdate >= date('1993-05-01')
  AND orders.orderdate < date('1993-08-01')
  AND lineitem.returnflag = 'R'
  AND customer.nationkey = nation.nationkey GROUP BY customer.custkey,
      customer.acctbal,
      customer.phone,
      nation.name,
      customer.address,
      customer.comment
ORDER BY revenue LIMIT 20
