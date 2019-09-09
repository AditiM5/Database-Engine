# README

#### This is a Disk file-based Database Engine which supports a subset of customized SQL-like query language allowing creation and dropping of tables, and bulk insertion of data.

## Sample Queries

1. SELECT SUM (ps.ps_supplycost), s.s_suppkey 
FROM part AS p, supplier AS s, partsupp AS ps 
WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0) 
GROUP BY s.s_suppkey
	
2. SELECT SUM (l.l_extendedprice * (1 - l.l_discount)), l.l_orderkey, o.o_orderdate, o.o_shippriority
FROM customer AS c, orders AS o, lineitem AS l 
WHERE (c.c_mktsegment = 'BUILDING') AND (c.c_custkey = o.o_custkey) AND (l.l_orderkey = o.o_orderkey) AND (l.l_orderkey < 100 OR o.o_orderkey < 100)
GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority

3. SELECT l.l_orderkey, l.l_partkey, l.l_suppkey 
FROM lineitem AS l 
WHERE (l.l_returnflag = 'R') AND (l.l_discount < 0.04 OR l.l_shipmode = 'MAIL')

4. SELECT DISTINCT c1.c_name, c1.c_address, c1.c_acctbal 
FROM customer AS c1, customer AS c2 
WHERE (c1.c_nationkey = c2.c_nationkey) AND (c1.c_name ='Customer#000070919')

5. SELECT SUM(l.l_discount) 
FROM customer AS c, orders AS o, lineitem AS l
WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) AND (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30.0) AND (l.l_discount < 0.03)


6. SELECT l.l_orderkey 
FROM lineitem AS l 
WHERE (l.l_quantity > 30.0)


7. SELECT DISTINCT c.c_name 
FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r 
WHERE (l.l_orderkey = o.o_orderkey) AND (o.o_custkey = c.c_custkey) AND (c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey)


8. SELECT l.l_discount 
FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r 
WHERE (l.l_orderkey = o.o_orderkey) AND (o.o_custkey = c.c_custkey) AND (c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey) AND (r.r_regionkey = 1) AND (o.o_orderkey < 10000)


### Gtest
#### Gtest file is called QueryPlanTest.cc
#### Running gtest file:

```make gtest```

To run the test, type

```./QueryPlanTest```

### Running gtest in docker:
```./docker.sh gtest```



### Authors

* **Aditi Malladi**
* **Suraj Kumar Reddy Thanugundla**
