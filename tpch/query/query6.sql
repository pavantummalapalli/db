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

SELECT
  SUM(lineitem.extendedprice * lineitem.discount) AS revenue
FROM lineitem
WHERE lineitem.shipdate >= DATE('1997-01-01')
AND lineitem.shipdate < DATE('1998-01-01')
AND lineitem.discount > 0.03
AND lineitem.discount < 0.05
AND lineitem.quantity < 25
