-- TPC-H 1
select
	l_returnflag,
	l_linestatus,
	sum(cast(l_quantity as bigint)) as sum_qty,
        sum(l_extendedprice / 100.0) as sum_base_price,
    	sum(l_extendedprice /100.0 * (1 - l_discount / 100.0)) as sum_disc_price,
    	sum(l_extendedprice /100.0 * (1 - l_discount / 100.0) * (1 + l_tax / 100.0)) as sum_charge,
	avg(cast(l_quantity as float)) as avg_qty,
  	avg(l_extendedprice / 100.0) as avg_price,
    	avg(l_discount / 100.0) as avg_disc,
	count(*) as count_order
from
lineitem
where	
l_shipdate <= '1998-09-16'
group by
l_returnflag,
l_linestatus
order by
	l_returnflag,
	l_linestatus;

-- TPC-H 2


drop view if exists q2_min_ps_supplycost;

create or replace table q2_min_ps_supplycost as
select 
	p_partkey as min_p_partkey,
	min(ps_supplycost) as min_ps_supplycost
from  region 
join nation on n_regionkey = r_regionkey   
join supplier on s_nationkey = n_nationkey
join partsupp on s_suppkey = ps_suppkey
join part on p_partkey = ps_partkey
where
 	r_name = 'EUROPE'
group by
	p_partkey;

select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from part join partsupp on p_partkey = ps_partkey
join supplier on s_suppkey = ps_suppkey
join nation on s_nationkey = n_nationkey
join region on n_regionkey = r_regionkey   
join q2_min_ps_supplycost on ps_supplycost = min_ps_supplycost and p_partkey = min_p_partkey
where
	low_selectivity(p_size = 37
	and p_type like '%COPPER')
	and r_name = 'EUROPE'
	
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;

-- TPC-H 3
select l_orderkey, sum(l_extendedprice / 100.0 * (1 - l_discount / 100.0)) as revenue, o_orderdate, o_shippriority 
from customer 
join orders on c_custkey = o_custkey 
join lineitem on l_orderkey = o_orderkey 
where low_selectivity(c_mktsegment = 'BUILDING') and o_orderdate < '1995-03-22'  
and l_shipdate > '1995-03-22' and l_shipdate <= dateadd(day,122,'1995-03-22' ) and o_orderdate > dateadd(day,-122,'1995-03-22' ) 
group by l_orderkey, o_orderdate, o_shippriority 
order by revenue desc, o_orderdate limit 10;

-- TPC-H 5
select sum(l_extendedprice / 100.0 * (1 - l_discount / 100.0)) as revenue ,n_name  
from  region 
join nation on n_regionkey = r_regionkey   
join customer on c_nationkey=n_nationkey  
join orders on  c_custkey = o_custkey   
join lineitem on l_orderkey = o_orderkey   
join supplier on l_suppkey = s_suppkey and c_nationkey =s_nationkey 
where  r_name = 'AFRICA'  and o_orderdate >= date '1993-01-01' AND o_orderdate < '1994-01-01'
and l_shipdate >= '1993-01-01' AND l_shipdate <= dateadd(day,122,'1994-01-01')   
GROUP BY n_name;

-- TPC-H 6
select sum( (l_extendedprice / 100.0) * (l_discount / 100.0) ) as revenue 
from lineitem 
where l_shipdate >= '1993-01-01' and l_shipdate < '1994-01-01' 
and cast(l_discount / 100.0 as float) between (0.06 - 0.01) 
and (0.06 + 0.01) and l_quantity < 25;

-- TPC-H 7
create or replace table q7_1 as 
 select n2.n_name as n_name,o_orderkey 
from nation n2  join customer on  c_nationkey = n2.n_nationkey     
join orders on c_custkey = o_custkey   
where  o_orderdate >= dateadd(day,-122,'1995-01-01') and o_orderdate <= '1996-12-31' and (n2.n_name = 'PERU' or n2.n_name = 'KENYA');

select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, q7_1.n_name as cust_nation, datepart(year,l_shipdate) as l_year, ( l_extendedprice / 100.0) * (1 - l_discount / 100.0) as volume     from    lineitem  join q7_1 on o_orderkey = l_orderkey    join supplier on s_suppkey = l_suppkey    join nation n1 on s_nationkey = n1.n_nationkey    where ((n1.n_name = 'KENYA'  and  q7_1.n_name = 'PERU') or (n1.n_name = 'PERU' and  q7_1.n_name = 'KENYA'))   and l_shipdate between '1995-01-01' and '1996-12-31'  ) as shipping     group by supp_nation, cust_nation, l_year   order by supp_nation, cust_nation, l_year;

-- TPC-H 8
select o_year, sum(case when nation = 'PERU' then volume else 0 end) / sum(volume) as mkt_share from 
( select datepart(year,o_orderdate) as o_year, l_extendedprice * (1 - l_discount / 100.0) as volume, n2.n_name as nation 
from  lineitem
join part on p_partkey = cast(l_partkey as int)
join orders on l_orderkey = o_orderkey
join customer on o_custkey = c_custkey
join nation n1 on c_nationkey = n1.n_nationkey
join region on  n1.n_regionkey = r_regionkey 
join supplier on s_suppkey = l_suppkey
join nation n2 on s_nationkey = n2.n_nationkey 
where  
r_name = 'AMERICA' 
and o_orderdate between '1995-01-01' and '1996-12-31' 
and l_shipdate >= '1995-01-01' and l_shipdate <= dateadd(day,122,'1996-12-31' )
and low_selectivity(p_type = 'ECONOMY BURNISHED NICKEL' )) as all_nations 
group by o_year order by o_year;

-- TPC-H 10
create or replace table tpch10 as
select c_custkey,n_name,  cast(sum(l_extendedprice / 100.0 * (1 - l_discount / 100.0)) as float) as revenue 
from lineitem join orders on l_orderkey = o_orderkey
join customer on c_custkey = o_custkey
join nation on c_nationkey = n_nationkey 
where o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' 
and l_returnflag = 'R' 
and l_shipdate >= '1993-07-01' and l_shipdate <= dateadd(day,122,'1993-10-01') 
group by c_custkey,n_name order by revenue desc limit 20 ;

select tpch10.*,c_acctbal,c_name,n_name,c_address, c_phone, c_comment from tpch10 join  customer on tpch10.c_custkey=customer.c_custkey;

-- TPC-H 11
drop view if exists q11_part_tmp_cached;
drop view if exists q11_sum_tmp_cached;

create view q11_part_tmp_cached as
select
	ps_partkey,
	sum( ( ps_supplycost / 100.0 ) * ps_availqty) as part_value
from
	nation join  supplier on s_nationkey = n_nationkey
	join partsupp on ps_suppkey = s_suppkey
	  
where	
	s_nationkey = n_nationkey
	and n_name = 'GERMANY'
group by ps_partkey;

create view q11_sum_tmp_cached as
select
	sum(part_value) as total_value
from
	q11_part_tmp_cached;

select
	ps_partkey, part_value as value
from (
	select
		ps_partkey,
		part_value,
		total_value
	from
		q11_part_tmp_cached join q11_sum_tmp_cached
on part_value > total_value * 0.0001
) a
	
order by
	value desc;
  
-- TPC-H 12
select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count 
from  lineitem join orders  on o_orderkey = l_orderkey 
where
low_selectivity(l_shipmode in ('REG AIR', 'MAIL') )
and l_commitdate < l_receiptdate 
and l_shipdate < l_commitdate 
and l_receiptdate >= '1995-01-01' and l_receiptdate < '1996-01-01' 
and l_shipdate >= dateadd(day,-30,'1995-01-01') and l_shipdate <  '1996-01-01' 
and o_orderdate >= dateadd(day,-152,'1995-01-01') and o_orderdate  <'1996-01-01'
group by l_shipmode order by l_shipmode;

-- TPC-H 14
select 100.00 * sum(case  when p_type like 'PROMO%'  then (l_extendedprice / 100.0) * (1 - l_discount / 100.0)  else 0  end) / sum( (l_extendedprice / 100.0) * (1 - l_discount / 100.0)) as promo_revenue  from lineitem join part on cast(l_partkey as int) = p_partkey  where l_shipdate >= '1995-08-01'and l_shipdate < '1995-09-01';

-- TPC-H 15
drop view if exists max_revenue_cached;

create or replace table revenue_cached as   
select  l_suppkey as supplier_no,  
sum( cast((l_extendedprice / 100.0 ) as float)* (1 - cast(l_discount as float) / 100.0)) as total_revenue  
from  lineitem  where low_selectivity(l_shipdate >= '1996-01-01'  and l_shipdate < '1996-04-01' ) 
group by l_suppkey;

create view max_revenue_cached as
select
	max(total_revenue) as max_revenue
from
	revenue_cached;

select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	revenue_cached join max_revenue_cached on  total_revenue = max_revenue 
	join supplier on s_suppkey = supplier_no

order by s_suppkey;

-- TPC-H 17
drop view if exists q17_lineitem_tmp_cached;

create or replace table part2 as 
select  p_partkey 
from part 
where p_brand = 'Brand#23' and p_container = 'MED BOX';

create view q17_lineitem_tmp_cached as
select
	l_partkey as t_partkey,
	0.2 * avg(l_quantity) as t_avg_quantity
from
	lineitem join (select distinct p_partkey from part2) 
	on l_partkey=cast(p_partkey as bigint)
group by l_partkey;

select
	sum(l_extendedprice / 100.0) / 7.0 as avg_yearly
from (
	select
		l_quantity,
		l_extendedprice,
		t_avg_quantity
	from
		q17_lineitem_tmp_cached join
		(select
			l_quantity,
			l_partkey,
			l_extendedprice
		from
			part2 join lineitem on cast(p_partkey as bigint) = l_partkey
		) l1 on l1.l_partkey = t_partkey
) a 
where l_quantity < t_avg_quantity;

-- TPC-H 18
create or replace table q18_tmp_cached as
select
	l_orderkey,
	sum(l_quantity) as t_sum_quantity
from
	lineitem
where
	l_orderkey is not null
group by
	l_orderkey;


select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from

	q18_tmp_cached t join orders on o_orderkey = t.l_orderkey
	join lineitem l on o_orderkey = l.l_orderkey
 	join customer on c_custkey = o_custkey

where
	 o_orderkey is not null
	and t.t_sum_quantity > 300
	and l.l_orderkey is not null
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate 
limit 100;

-- TPC-H 19
select
	sum((l_extendedprice / 100.0) * (1 - l_discount / 100.0)) as revenue
from
	part join lineitem on p_partkey = cast(l_partkey as int)
	
where
	low_selectivity ((
		
		 p_brand = 'Brand#32'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 7 and l_quantity <= 7 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(

		 p_brand = 'Brand#35'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 15 and l_quantity <= 15 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(

		p_brand = 'Brand#24'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 26 and l_quantity <= 26 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	));
  
  -- TPC-H 20
  
  create or replace table q20_tmp1_cached as
select distinct p_partkey
from
	part
where
	p_name like 'forest%';

create or replace table q20_tmp2_cached as
select
	l_partkey,
	l_suppkey,
	cast(0.5 * sum(l_quantity) as float) as sum_quantity
from
	lineitem join q20_tmp1_cached
	 on cast(p_partkey as bigint) = l_partkey 
where
	l_shipdate >= '1994-01-01'
	and l_shipdate < '1995-01-01'
group by l_partkey, l_suppkey;

create or replace table q20_tmp3_cached as
select
	ps_suppkey,
	ps_availqty,
	sum_quantity
from
	 q20_tmp1_cached
	join partsupp on ps_partkey = p_partkey
	join  q20_tmp2_cached on ps_partkey = p_partkey 
		and cast(ps_partkey as bigint) = l_partkey 
		and ps_suppkey = l_suppkey;


create or replace table q20_tmp4_cached as
select
	ps_suppkey
from
	q20_tmp3_cached
where
	ps_availqty > sum_quantity
group by ps_suppkey;

select
	s_name,
	s_address
from
  	supplier join nation on s_nationkey = n_nationkey        
	join q20_tmp4_cached on s_suppkey = ps_suppkey
where

	 n_name = 'CANADA'
order by s_name;


-- TPC-H 22
drop view if exists q22_customer_tmp1_cached;
drop view if exists q22_orders_tmp_cached;

create or replace table q22_customer_tmp_cached as
select
	cast(c_acctbal / 100.0 as float) as c_acctbal,
	c_custkey,
	substring(c_phone, 1, 2) as cntrycode
from
	customer
where
	substring(c_phone, 1, 2) = '13' or
	substring(c_phone, 1, 2) = '31' or
	substring(c_phone, 1, 2) = '23' or
	substring(c_phone, 1, 2) = '29' or
	substring(c_phone, 1, 2) = '30' or
	substring(c_phone, 1, 2) = '18' or
	substring(c_phone, 1, 2) = '17';
 
create view  q22_customer_tmp1_cached as
select
	avg(c_acctbal) as avg_acctbal
from
	q22_customer_tmp_cached
where
	c_acctbal > 0.00;


create or replace table  q22_orders_tmp_cached as
select
	o_custkey
from
	orders join q22_customer_tmp_cached on c_custkey = o_custkey	
group by
	o_custkey;

select
	cntrycode,
	count(1) as numcust,
	sum(c_acctbal) as totacctbal
from (
	select
		cntrycode,
		c_acctbal,
		avg_acctbal
	from
		q22_customer_tmp1_cached ct1 join (
			select
				cntrycode,
				c_acctbal
			from
				q22_orders_tmp_cached ot
				right outer join q22_customer_tmp_cached ct
				on ct.c_custkey = ot.o_custkey
			where
				o_custkey is null
		) ct2
on 	c_acctbal > avg_acctbal
) a


group by
	cntrycode
order by
	cntrycode;
