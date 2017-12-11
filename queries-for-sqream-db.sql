-- TPC-H 1
SELECT l_returnflag,
       l_linestatus,
       Sum(Cast(l_quantity AS BIGINT)) AS sum_qty,
       Sum(l_extendedprice / 100.0) AS sum_base_price,
       Sum(l_extendedprice / 100.0 * (1 - l_discount / 100.0)) AS sum_disc_price,
       Sum(l_extendedprice / 100.0 * (1 - l_discount / 100.0) * (1 + l_tax / 100.0)) AS sum_charge,
       Avg(Cast(l_quantity AS FLOAT)) AS avg_qty,
       Avg(l_extendedprice / 100.0) AS avg_price,
       Avg(l_discount / 100.0) AS avg_disc,
       Count(*) AS count_order
FROM lineitem
WHERE l_shipdate <= '1998-09-16'
GROUP BY l_returnflag,
         l_linestatus
ORDER BY l_returnflag,
         l_linestatus;


-- TPC-H 2
DROP VIEW IF EXISTS q2_min_ps_supplycost;


CREATE OR REPLACE TABLE q2_min_ps_supplycost AS
SELECT p_partkey AS min_p_partkey,
       min(ps_supplycost) AS min_ps_supplycost
FROM region
JOIN nation ON n_regionkey = r_regionkey
JOIN supplier ON s_nationkey = n_nationkey
JOIN partsupp ON s_suppkey = ps_suppkey
JOIN part ON p_partkey = ps_partkey
WHERE r_name = 'EUROPE'
GROUP BY p_partkey;


SELECT s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
FROM part
JOIN partsupp ON p_partkey = ps_partkey
JOIN supplier ON s_suppkey = ps_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
JOIN q2_min_ps_supplycost ON ps_supplycost = min_ps_supplycost
AND p_partkey = min_p_partkey
WHERE low_selectivity(p_size = 37
                      AND p_type LIKE '%COPPER')
  AND r_name = 'EUROPE'
ORDER BY s_acctbal DESC,
         n_name,
         s_name,
         p_partkey
LIMIT 100;

-- TPC-H 3
SELECT l_orderkey,
       sum(l_extendedprice / 100.0 * (1 - l_discount / 100.0)) AS revenue,
       o_orderdate,
       o_shippriority
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
WHERE low_selectivity(c_mktsegment = 'BUILDING')
  AND o_orderdate < '1995-03-22'
  AND l_shipdate > '1995-03-22'
  AND l_shipdate <= dateadd(DAY,122,'1995-03-22')
  AND o_orderdate > dateadd(DAY,-122,'1995-03-22')
GROUP BY l_orderkey,
         o_orderdate,
         o_shippriority
ORDER BY revenue DESC,
         o_orderdate
LIMIT 10;

-- TPC-H 5
ELECT sum(l_extendedprice / 100.0 * (1 - l_discount / 100.0)) AS revenue,
       n_name
FROM region
JOIN nation ON n_regionkey = r_regionkey
JOIN customer ON c_nationkey=n_nationkey
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
JOIN supplier ON l_suppkey = s_suppkey
AND c_nationkey =s_nationkey
WHERE r_name = 'AFRICA'
  AND o_orderdate >= date '1993-01-01'
  AND o_orderdate < '1994-01-01'
  AND l_shipdate >= '1993-01-01'
  AND l_shipdate <= dateadd(DAY,122,'1994-01-01')
GROUP BY n_name;

-- TPC-H 6
SELECT sum((l_extendedprice / 100.0) * (l_discount / 100.0)) AS revenue
FROM lineitem
WHERE l_shipdate >= '1993-01-01'
  AND l_shipdate < '1994-01-01'
  AND cast(l_discount / 100.0 AS float) BETWEEN (0.06 - 0.01) AND (0.06 + 0.01)
  AND l_quantity < 25;

-- TPC-H 7
CREATE OR REPLACE TABLE q7_1 AS
SELECT n2.n_name AS n_name,
       o_orderkey
FROM nation n2
JOIN customer ON c_nationkey = n2.n_nationkey
JOIN orders ON c_custkey = o_custkey
WHERE o_orderdate >= dateadd(DAY,-122,'1995-01-01')
  AND o_orderdate <= '1996-12-31'
  AND (n2.n_name = 'PERU'
       OR n2.n_name = 'KENYA');


SELECT supp_nation,
       cust_nation,
       l_year,
       sum(volume) AS revenue
FROM
  (SELECT n1.n_name AS supp_nation,
          q7_1.n_name AS cust_nation,
          datepart(YEAR,l_shipdate) AS l_year,
          (l_extendedprice / 100.0) * (1 - l_discount / 100.0) AS volume
   FROM lineitem
   JOIN q7_1 ON o_orderkey = l_orderkey
   JOIN supplier ON s_suppkey = l_suppkey
   JOIN nation n1 ON s_nationkey = n1.n_nationkey
   WHERE ((n1.n_name = 'KENYA'
           AND q7_1.n_name = 'PERU')
          OR (n1.n_name = 'PERU'
              AND q7_1.n_name = 'KENYA'))
     AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31' ) AS shipping
GROUP BY supp_nation,
         cust_nation,
         l_year
ORDER BY supp_nation,
         cust_nation,
         l_year;
	 
	 
-- TPC-H 8
SELECT o_year,
       sum(CASE
               WHEN nation = 'PERU' THEN volume
               ELSE 0
           END) / sum(volume) AS mkt_share
FROM
  (SELECT datepart(YEAR,o_orderdate) AS o_year,
          l_extendedprice * (1 - l_discount / 100.0) AS volume,
          n2.n_name AS nation
   FROM lineitem
   JOIN part ON p_partkey = cast(l_partkey AS int)
   JOIN orders ON l_orderkey = o_orderkey
   JOIN customer ON o_custkey = c_custkey
   JOIN nation n1 ON c_nationkey = n1.n_nationkey
   JOIN region ON n1.n_regionkey = r_regionkey
   JOIN supplier ON s_suppkey = l_suppkey
   JOIN nation n2 ON s_nationkey = n2.n_nationkey
   WHERE r_name = 'AMERICA'
     AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
     AND l_shipdate >= '1995-01-01'
     AND l_shipdate <= dateadd(DAY,122,'1996-12-31')
     AND low_selectivity(p_type = 'ECONOMY BURNISHED NICKEL')) AS all_nations
GROUP BY o_year
ORDER BY o_year;

-- TPC-H 10
CREATE OR REPLACE TABLE tpch10 AS
SELECT c_custkey,
       n_name,
       cast(sum(l_extendedprice / 100.0 * (1 - l_discount / 100.0)) AS float) AS revenue
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON c_custkey = o_custkey
JOIN nation ON c_nationkey = n_nationkey
WHERE o_orderdate >= '1993-07-01'
  AND o_orderdate < '1993-10-01'
  AND l_returnflag = 'R'
  AND l_shipdate >= '1993-07-01'
  AND l_shipdate <= dateadd(DAY,122,'1993-10-01')
GROUP BY c_custkey,
         n_name
ORDER BY revenue DESC
LIMIT 20 ;


SELECT tpch10.*,
       c_acctbal,
       c_name,
       n_name,
       c_address,
       c_phone,
       c_comment
FROM tpch10
JOIN customer ON tpch10.c_custkey=customer.c_custkey;


-- TPC-H 11
DROP VIEW IF EXISTS q11_part_tmp_cached;


DROP VIEW IF EXISTS q11_sum_tmp_cached;


CREATE VIEW q11_part_tmp_cached AS
SELECT ps_partkey,
       sum((ps_supplycost / 100.0) * ps_availqty) AS part_value
FROM nation
JOIN supplier ON s_nationkey = n_nationkey
JOIN partsupp ON ps_suppkey = s_suppkey
WHERE s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY ps_partkey;


CREATE VIEW q11_sum_tmp_cached AS
SELECT sum(part_value) AS total_value
FROM q11_part_tmp_cached;


SELECT ps_partkey,
       part_value AS value
FROM
  ( SELECT ps_partkey,
           part_value,
           total_value
   FROM q11_part_tmp_cached
   JOIN q11_sum_tmp_cached ON part_value > total_value * 0.0001) a
ORDER BY value DESC;
  
-- TPC-H 12
SELECT l_shipmode,
       sum(CASE
               WHEN o_orderpriority = '1-URGENT'
                    OR o_orderpriority = '2-HIGH' THEN 1
               ELSE 0
           END) AS high_line_count,
       sum(CASE
               WHEN o_orderpriority <> '1-URGENT'
                    AND o_orderpriority <> '2-HIGH' THEN 1
               ELSE 0
           END) AS low_line_count
FROM lineitem
JOIN orders ON o_orderkey = l_orderkey
WHERE low_selectivity(l_shipmode IN ('REG AIR', 'MAIL'))
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= '1995-01-01'
  AND l_receiptdate < '1996-01-01'
  AND l_shipdate >= dateadd(DAY,-30,'1995-01-01')
  AND l_shipdate < '1996-01-01'
  AND o_orderdate >= dateadd(DAY,-152,'1995-01-01')
  AND o_orderdate <'1996-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode;


-- TPC-H 14
SELECT 100.00 * sum(CASE
                        WHEN p_type LIKE 'PROMO%' THEN (l_extendedprice / 100.0) * (1 - l_discount / 100.0)
                        ELSE 0
                    END) / sum((l_extendedprice / 100.0) * (1 - l_discount / 100.0)) AS promo_revenue
FROM lineitem
JOIN part ON cast(l_partkey AS int) = p_partkey
WHERE l_shipdate >= '1995-08-01'
  AND l_shipdate < '1995-09-01';


-- TPC-H 15
DROP VIEW IF EXISTS max_revenue_cached;


CREATE OR REPLACE TABLE revenue_cached AS
SELECT l_suppkey AS supplier_no,
       sum(cast((l_extendedprice / 100.0) AS float)* (1 - cast(l_discount AS float) / 100.0)) AS total_revenue
FROM lineitem
WHERE low_selectivity(l_shipdate >= '1996-01-01'
                      AND l_shipdate < '1996-04-01')
GROUP BY l_suppkey;


CREATE VIEW max_revenue_cached AS
SELECT max(total_revenue) AS max_revenue
FROM revenue_cached;


SELECT s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
FROM revenue_cached
JOIN max_revenue_cached ON total_revenue = max_revenue
JOIN supplier ON s_suppkey = supplier_no
ORDER BY s_suppkey;

-- TPC-H 17
DROP VIEW IF EXISTS q17_lineitem_tmp_cached;


CREATE OR REPLACE TABLE part2 AS
SELECT p_partkey
FROM part
WHERE p_brand = 'Brand#23'
  AND p_container = 'MED BOX';


CREATE VIEW q17_lineitem_tmp_cached AS
SELECT l_partkey AS t_partkey,
       0.2 * avg(l_quantity) AS t_avg_quantity
FROM lineitem
JOIN
  (SELECT DISTINCT p_partkey
   FROM part2) ON l_partkey=cast(p_partkey AS bigint)
GROUP BY l_partkey;


SELECT sum(l_extendedprice / 100.0) / 7.0 AS avg_yearly
FROM
  ( SELECT l_quantity,
           l_extendedprice,
           t_avg_quantity
   FROM q17_lineitem_tmp_cached
   JOIN
     (SELECT l_quantity,
             l_partkey,
             l_extendedprice
      FROM part2
      JOIN lineitem ON cast(p_partkey AS bigint) = l_partkey ) l1 ON l1.l_partkey = t_partkey) a
WHERE l_quantity < t_avg_quantity;

-- TPC-H 18
CREATE OR REPLACE TABLE q18_tmp_cached AS
SELECT l_orderkey,
       sum(l_quantity) AS t_sum_quantity
FROM lineitem
WHERE l_orderkey IS NOT NULL
GROUP BY l_orderkey;


SELECT c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
FROM q18_tmp_cached t
JOIN orders ON o_orderkey = t.l_orderkey
JOIN lineitem l ON o_orderkey = l.l_orderkey
JOIN customer ON c_custkey = o_custkey
WHERE o_orderkey IS NOT NULL
  AND t.t_sum_quantity > 300
  AND l.l_orderkey IS NOT NULL
GROUP BY c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
ORDER BY o_totalprice DESC,
         o_orderdate
LIMIT 100;

-- TPC-H 19
SELECT sum((l_extendedprice / 100.0) * (1 - l_discount / 100.0)) AS revenue
FROM part
JOIN lineitem ON p_partkey = cast(l_partkey AS int)
WHERE low_selectivity (( p_brand = 'Brand#32'
                        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                        AND l_quantity >= 7
                        AND l_quantity <= 7 + 10
                        AND p_size BETWEEN 1 AND 5
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON' )
                       OR ( p_brand = 'Brand#35'
                           AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                           AND l_quantity >= 15
                           AND l_quantity <= 15 + 10
                           AND p_size BETWEEN 1 AND 10
                           AND l_shipmode IN ('AIR', 'AIR REG')
                           AND l_shipinstruct = 'DELIVER IN PERSON' )
                       OR ( p_brand = 'Brand#24'
                           AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                           AND l_quantity >= 26
                           AND l_quantity <= 26 + 10
                           AND p_size BETWEEN 1 AND 15
                           AND l_shipmode IN ('AIR', 'AIR REG')
                           AND l_shipinstruct = 'DELIVER IN PERSON' ));
  
  -- TPC-H 20
  

CREATE OR REPLACE TABLE q20_tmp1_cached AS
SELECT DISTINCT p_partkey
FROM part
WHERE p_name LIKE 'forest%';


CREATE OR REPLACE TABLE q20_tmp2_cached AS
SELECT l_partkey,
       l_suppkey,
       cast(0.5 * sum(l_quantity) AS float) AS sum_quantity
FROM lineitem
JOIN q20_tmp1_cached ON cast(p_partkey AS bigint) = l_partkey
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate < '1995-01-01'
GROUP BY l_partkey,
         l_suppkey;


CREATE OR REPLACE TABLE q20_tmp3_cached AS
SELECT ps_suppkey,
       ps_availqty,
       sum_quantity
FROM q20_tmp1_cached
JOIN partsupp ON ps_partkey = p_partkey
JOIN q20_tmp2_cached ON ps_partkey = p_partkey
AND cast(ps_partkey AS bigint) = l_partkey
AND ps_suppkey = l_suppkey;


CREATE OR REPLACE TABLE q20_tmp4_cached AS
SELECT ps_suppkey
FROM q20_tmp3_cached
WHERE ps_availqty > sum_quantity
GROUP BY ps_suppkey;


SELECT s_name,
       s_address
FROM supplier
JOIN nation ON s_nationkey = n_nationkey
JOIN q20_tmp4_cached ON s_suppkey = ps_suppkey
WHERE n_name = 'CANADA'
ORDER BY s_name;


-- TPC-H 22
DROP VIEW IF EXISTS q22_customer_tmp1_cached;


DROP VIEW IF EXISTS q22_orders_tmp_cached;


CREATE OR REPLACE TABLE q22_customer_tmp_cached AS
SELECT cast(c_acctbal / 100.0 AS float) AS c_acctbal,
       c_custkey,
       substring(c_phone, 1, 2) AS cntrycode
FROM customer
WHERE substring(c_phone, 1, 2) = '13'
  OR substring(c_phone, 1, 2) = '31'
  OR substring(c_phone, 1, 2) = '23'
  OR substring(c_phone, 1, 2) = '29'
  OR substring(c_phone, 1, 2) = '30'
  OR substring(c_phone, 1, 2) = '18'
  OR substring(c_phone, 1, 2) = '17';


CREATE VIEW q22_customer_tmp1_cached AS
SELECT avg(c_acctbal) AS avg_acctbal
FROM q22_customer_tmp_cached
WHERE c_acctbal > 0.00;


CREATE OR REPLACE TABLE q22_orders_tmp_cached AS
SELECT o_custkey
FROM orders
JOIN q22_customer_tmp_cached ON c_custkey = o_custkey
GROUP BY o_custkey;


SELECT cntrycode,
       count(1) AS numcust,
       sum(c_acctbal) AS totacctbal
FROM
  ( SELECT cntrycode,
           c_acctbal,
           avg_acctbal
   FROM q22_customer_tmp1_cached ct1
   JOIN
     ( SELECT cntrycode,
              c_acctbal
      FROM q22_orders_tmp_cached ot
      RIGHT OUTER JOIN q22_customer_tmp_cached ct ON ct.c_custkey = ot.o_custkey
      WHERE o_custkey IS NULL ) ct2 ON c_acctbal > avg_acctbal) a
GROUP BY cntrycode
ORDER BY cntrycode;
