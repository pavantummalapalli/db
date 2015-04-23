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

--SELECT n.name, SUM(l.extendedprice * (1 - l.discount)) AS revenue 
--FROM   customer c, orders o, lineitem l, supplier s, nation n, region r
--WHERE  c.custkey = o.custkey
--  AND  l.orderkey = o.orderkey 
--  AND  l.suppkey = s.suppkey
--  AND  c.nationkey = s.nationkey 
--  AND  s.nationkey = n.nationkey 
--  AND  n.regionkey = r.regionkey 
--  AND  r.name = 'ASIA'
--  AND  o.orderdate >= DATE('1994-01-01')
--  AND  o.orderdate <  DATE('1995-01-01')
-- GROUP BY n.name

SELECT nation.name,
       sum(lineitem.extendedprice * (1 - lineitem.discount)) AS revenue
FROM region,
     nation,
     customer,
     orders,
     lineitem,
     supplier
WHERE customer.custkey = orders.custkey
  AND lineitem.orderkey = orders.orderkey
  AND lineitem.suppkey = supplier.suppkey
  AND customer.nationkey = nation.nationkey
  AND supplier.nationkey = nation.nationkey
  AND nation.regionkey = region.regionkey
  AND region.name = 'EUROPE'
  AND orders.orderdate >= DATE('1997-01-01')
  AND orders.orderdate < DATE('1998-01-01')
GROUP BY nation.name
ORDER BY revenue DESC
