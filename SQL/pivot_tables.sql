/* This code is part of my tutorial on pivot tables in SQL
   Blog post: habr.com/ru/post/506070/ (in Russian)
*/


--- Division 0: Create testing data ---

-- create table
create table test_supply (supplier varchar(10) null, -- varchar2(10) in Oracle
                          product varchar(10) null,  -- varchar2(10) in Oracle
                          volume int null
                          ); 

-- insert test data
insert into test_supply (supplier, product, volume) values ('A', 'Product 1', 928);
insert into test_supply (supplier, product, volume) values ('A', 'Product 1', 422);
insert into test_supply (supplier, product, volume) values ('A', 'Product 4', 164);
insert into test_supply (supplier, product, volume) values ('A', 'Product 1', 403);
insert into test_supply (supplier, product, volume) values ('A', 'Product 3', 26);
insert into test_supply (supplier, product, volume) values ('B', 'Product 4', 594);
insert into test_supply (supplier, product, volume) values ('B', 'Product 4', 989);
insert into test_supply (supplier, product, volume) values ('B', 'Product 3', 844);
insert into test_supply (supplier, product, volume) values ('B', 'Product 4', 870);
insert into test_supply (supplier, product, volume) values ('B', 'Product 2', 644);
insert into test_supply (supplier, product, volume) values ('C', 'Product 2', 733);
insert into test_supply (supplier, product, volume) values ('C', 'Product 2', 502);
insert into test_supply (supplier, product, volume) values ('C', 'Product 1', 97);
insert into test_supply (supplier, product, volume) values ('C', 'Product 3', 620);
insert into test_supply (supplier, product, volume) values ('C', 'Product 2', 776);

-- check results
select * from test_supply;

--- Division 1: Straightforward solution ---

-- ANSI SQL (no totals)
select t.product, 
       sum(case when t.supplier = 'A' then t.volume end) as A,
       sum(case when t.supplier = 'B' then t.volume end) as B,
       sum(case when t.supplier = 'C' then t.volume end) as C
from test_supply t
group by t.product;

-- ANSI SQL (with totals)
select coalesce(t.product, 'total_sum') as product,
       sum(case when t.supplier = 'A' then t.volume end) as A,
       sum(case when t.supplier = 'B' then t.volume end) as B,
       sum(case when t.supplier = 'C' then t.volume end) as C,
       sum(t.volume) as total_sum
from test_supply t
group by rollup(t.product);

-- ANSI SQL (with totals, old RDMS where rollup is not available)
select t.product,
       sum(case when t.supplier = 'A' then t.volume end) as A,
       sum(case when t.supplier = 'B' then t.volume end) as B,
       sum(case when t.supplier = 'C' then t.volume end) as C,
       sum(t.volume) as total_sum
from test_supply t
group by t.product
union all
select 'total_sum',
       sum(case when t.supplier = 'A' then t.volume end),
       sum(case when t.supplier = 'B' then t.volume end),
       sum(case when t.supplier = 'C' then t.volume end),
       sum(t.volume) as total_sum
from test_supply t
;

-- MySQL: IF
select coalesce(t.product, 'total_sum') as product, 
       sum(IF(t.supplier = 'A', t.volume, null)) as A,
       sum(IF(t.supplier = 'B', t.volume, null)) as B,
       sum(IF(t.supplier = 'C', t.volume, null)) as C,
       sum(t.volume) as total_sum
from test_supply t
group by rollup(t.product);

-- Oracle: DECODE
select coalesce(t.product, 'total_sum') as product, 
       sum(decode(t.supplier, 'A', t.volume, null)) as A,
       sum(decode(t.supplier, 'B', t.volume, null)) as B,
       sum(decode(t.supplier, 'C', t.volume, null)) as C,
       sum(t.volume) as total_sum
from test_supply t
group by rollup(t.product);

-- SQL Server 2012 or higher: IIF
select coalesce(t.product, 'total_sum') as product, 
       sum(iif(t.supplier = 'A', t.volume, null)) as A,
       sum(iif(t.supplier = 'B', t.volume, null)) as B,
       sum(iif(t.supplier = 'C', t.volume, null)) as C,
       sum(t.volume) as total_sum
from test_supply t
group by rollup(t.product);

-- PostgreSQL: FILTER
select coalesce(t.product, 'total_sum') as product,
       sum(t.volume) filter (where t.supplier = 'A') as A,
       sum(t.volume) filter (where t.supplier = 'B') as B,
       sum(t.volume) filter (where t.supplier = 'C') as C,
       sum(t.volume) as total_sum
from test_supply t
group by rollup(t.product);


--- Division 2: PIVOT clause (SQL Server, Oracle) ---

-- without totals
select *
from
	(
	select t.supplier as supplier, 
	       t.product as product, 
	       sum(t.volume) as agg
	from test_supply t
	group by t.supplier, t.product
	) t
pivot (sum(agg) 
        -- NB: SQL Server: double quotes, Oracle DB: single quotes
       for supplier in ("A", "B", "C")
	   ) pvt	
;

-- with totals
select *
from
	(
	select coalesce(t.supplier, 'total_sum') as supplier, 
	       coalesce(t.product, 'total_sum') as product, 
	       sum(t.volume) as agg
	from test_supply t
	group by cube(t.supplier, t.product)
	) as t
pivot (sum(agg) 
       -- SQL Server: double quotes, Oracle DB: single quotes
       for supplier in ("A", "B", "C", "total_sum")
	   ) as pvt	
;


--- Division 3: crosstab function (PostgreSQL) ---

-- crosstab is part of tablefunc extension 
-- use create extension in PostgreSQL 9.1+
create extension tablefunc;

-- without totals
select * from crosstab 
    (
    $$select t.product as product,
	     t.supplier as supplier,	         	         
	     sum(t.volume) as agg
      from test_supply t
      group by t.supplier, t.product
      order by product, supplier $$,
    $$ select distinct tt.supplier as supplier
      from test_supply tt
      order by supplier $$
     )
   as cst("product" varchar, "A" bigint, "B" bigint, "C" bigint);

-- with totals
select *
from crosstab 
    (
    $$select coalesce(t.product, 'total_sum') as product,
	     coalesce(t.supplier, 'total_sum') as supplier,	         	         
	     sum(t.volume) as agg
      from test_supply t
      group by cube(t.supplier, t.product)
      order by product, supplier $$,
    $$ (select distinct tt.supplier as supplier
      from test_supply tt
      order by supplier)
      union all
      select 'total_sum' $$
     )
   as cst("product" varchar, "A" bigint, "B" bigint, "C" bigint, "total_sum" bigint);


--- Division 4: common table expression with self joins ---

-- without totals
with cte
as	(
	select t.supplier, 
	       t.product, 
	       sum(t.volume) as agg
	from test_supply t
	group by t.supplier, t.product
	)
select distinct t.product, 
                a.agg as A,
                b.agg as B,
                c.agg as C
from cte t
left join cte a
	on t.product = a.product
		and a.supplier = 'A'
left join cte b
	on t.product = b.product
		and b.supplier = 'B'
left join cte c
	on t.product = c.product
		and c.supplier = 'C'
order by product;

-- with totals
with cte
as	(
	select coalesce(t.supplier, 'total_sum') as supplier, 
	       coalesce(t.product, 'total_sum') as product, 
	       sum(t.volume) as agg
	from test_supply t
	group by cube(t.supplier, t.product)
	)
select distinct t.product, 
                a.agg as A,
                b.agg as B,
                c.agg as C,
                ts.agg as total_sum
from cte t
left join cte a
	on t.product = a.product
		and a.supplier = 'A'
left join cte b
	on t.product = b.product
		and b.supplier = 'B'
left join cte c
	on t.product = c.product
		and c.supplier = 'C'
left join cte ts
	on t.product = ts.product
		and ts.supplier = 'total_sum'
order by product;


--- Division 5: dynamic SQL (example for SQL Server / T-SQL only) ---

-- with PIVOT
declare @colnames as nvarchar(max),
        @query as nvarchar(max);
select @colnames = stuff((select distinct ', ' + '"' + t.supplier + '"'
			  from test_supply t
			  for xml path ('')
			  ), 1, 1, ''
			 ) + ', "total_sum"';
set @query =   'select *
		from
			(
			select coalesce(t.supplier, ''total_sum'') as supplier, 
					coalesce(t.product, ''total_sum'') as product, 
					sum(t.volume) as agg
			from test_supply t
			group by cube(t.supplier, t.product)
			) as t
		pivot (sum(agg) 
				for supplier in (' + @colnames + ')
				) as pvt';
execute(@query);

-- basic solution without PIVOT
select distinct supplier into #colnames from test_supply;
declare @colname as nvarchar(max),
        @query as nvarchar(max);
set @query = 'select coalesce(t.product, ''total_sum'') as product';
while exists (select * from #colnames)
begin
	select top 1 @colname = supplier from #colnames;
	delete from #colnames where supplier = @colname;
	set @query = @query + 
                     ', sum(case when t.supplier = ''' + 
                     @colname + 
                     ''' then t.volume end) as ' + 
                     @colname
end;
set @query = @query + ' , sum(t.volume) as total_sum
                       from test_supply t
                       group by rollup(t.product)'

drop table #colnames;
execute(@query);