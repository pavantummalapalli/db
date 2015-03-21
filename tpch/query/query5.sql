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

CREATE TABLE ORDERS (
        orderkey      INT,
        custkey        INT,
        orderstatus    CHAR(1),
        totalprice     DECIMAL,
        orderdate      DATE,
        orderpriority  CHAR(15),
        clerk          CHAR(15),
        shippriority   INT,
        comment        VARCHAR(79)
);

CREATE TABLE CUSTOMER (
        custkey      INT,
        name         VARCHAR(25),
        address      VARCHAR(40),
        nationkey    INT,
        phone        CHAR(15),
        acctbal      DECIMAL,
        mktsegment   CHAR(10),
        comment      VARCHAR(117)
);

CREATE TABLE SUPPLIER (
        suppkey      INT,
        name         CHAR(25),
        address      VARCHAR(40),
        nationkey    INT,
        phone        CHAR(15),
        acctbal      DECIMAL,
        comment      VARCHAR(101)
);

CREATE TABLE NATION (
        nationkey    INT,
        name         CHAR(25),
        regionkey    INT,
        comment      VARCHAR(152)
);
  
CREATE TABLE REGION (
        regionkey    INT,
        name         CHAR(25),
        comment      VARCHAR(152)
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
