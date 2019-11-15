/* This code is part of my tutorial on running total (cumulative sum, running sum) in SQL
   Blog post: habr.com/ru/post/474458/ (in Russian)
*/


--- Division 0: Create testing data ---

-- simpliest case: plain data, no groups
create table test_simple (dt date null,
                          val int null
                         ); 

-- insert some data // warning: dates in ANSI format
-- use appropriate date format in your DMBS (or change settings, e.g. using NLS_DATE_FORMAT in Oracle)
insert into test_simple (dt, val) values ('2019-11-01', 6);
insert into test_simple (dt, val) values ('2019-11-02', 3);
insert into test_simple (dt, val) values ('2019-11-03', 3);
insert into test_simple (dt, val) values ('2019-11-04', 4);
insert into test_simple (dt, val) values ('2019-11-05', 2);
insert into test_simple (dt, val) values ('2019-11-06', 4);
insert into test_simple (dt, val) values ('2019-11-07', 8);
insert into test_simple (dt, val) values ('2019-11-08', 0);
insert into test_simple (dt, val) values ('2019-11-09', 6);
insert into test_simple (dt, val) values ('2019-11-10', 0);
insert into test_simple (dt, val) values ('2019-11-11', 8);
insert into test_simple (dt, val) values ('2019-11-12', 8);
insert into test_simple (dt, val) values ('2019-11-13', 0);
insert into test_simple (dt, val) values ('2019-11-14', 2);
insert into test_simple (dt, val) values ('2019-11-15', 8);
insert into test_simple (dt, val) values ('2019-11-16', 7);

-- check data
select * from test_simple order by dt;


--sophisticated case: grouped data
create table test_groups (grp varchar null, -- varchar2(1) in Oracle, etc
                          dt date null,
                          val int null
                          );

-- insert some data // warning: dates in ANSI format
-- use appropriate date format in your DMBS (or change settings, e.g. using NLS_DATE_FORMAT in Oracle)
insert into test_groups (grp, dt, val) values ('a', '2019-11-06', 1);
insert into test_groups (grp, dt, val) values ('a', '2019-11-07', 3);
insert into test_groups (grp, dt, val) values ('a', '2019-11-08', 4);
insert into test_groups (grp, dt, val) values ('a', '2019-11-09', 1);
insert into test_groups (grp, dt, val) values ('a', '2019-11-10', 7);
insert into test_groups (grp, dt, val) values ('b', '2019-11-06', 9);
insert into test_groups (grp, dt, val) values ('b', '2019-11-07', 10);
insert into test_groups (grp, dt, val) values ('b', '2019-11-08', 9);
insert into test_groups (grp, dt, val) values ('b', '2019-11-09', 1);
insert into test_groups (grp, dt, val) values ('b', '2019-11-10', 10);
insert into test_groups (grp, dt, val) values ('c', '2019-11-06', 4);
insert into test_groups (grp, dt, val) values ('c', '2019-11-07', 10);
insert into test_groups (grp, dt, val) values ('c', '2019-11-08', 9);
insert into test_groups (grp, dt, val) values ('c', '2019-11-09', 4);
insert into test_groups (grp, dt, val) values ('c', '2019-11-10', 4);

-- check data
select * from test_groups order by grp, dt;



--- Division 1: Solutions for simple case (no groups) ---

-- Window functions
select s.*,
       coalesce(sum(s.val) over (order by s.dt 
                rows between unbounded preceding and current row), 
                0) as total
from test_simple s
order by s.dt;

-- Subquary
select s.*,
       (select coalesce(sum(t2.val), 0)
	from test_simple t2
	where t2.dt <= s.dt) as total
from test_simple s
order by s.dt;

-- Inner join
select s.*, 
       coalesce(sum(t2.val), 0) as total
from test_simple s
inner join test_simple t2
	 on t2.dt <= s.dt
group by s.dt, 
	 s.val
order by s.dt;

-- Cartesian join
select s.*, 
       coalesce(sum(t2.val), 0) as total
from test_simple s,
     test_simple t2
where t2.dt <= s.dt
group by s.dt, 
	 s.val
order by s.dt;


-- Recursive query
-- required condition: no gaps in dt field
with cte (dt,
          val,
          total)
as
   (select dt,
	   val, 
	   val as total
    from test_simple
    where dt = (select min(dt) from test_simple)
			
    union all
			
    select r.dt,
	   r.val,
	   cte.total + r.val
    from cte
    inner join test_simple r
    	-- r.dt = dateadd(day, 1, cte.dt) in SQL Server, r.dt = cte.dt + 1 in Oracle, etc
      on r.dt = dateadd(day, 1, cte.dt)
    )
select dt,
       val, 
       total 
from cte
order by dt;


-- Recursive query & window function row_number
-- here gaps in dt field became acceptable
with cte1 (dt,
           val,
	   rn)
as (select dt,
           val,
	   row_number() over (order by dt) as rn
	from test_simple),
cte2 (dt,
      val,
      rn,
      total)
as
   (select dt,
	   val,
	   rn,
	   val as total
    from cte1
    where rn = 1
			
    union all
			
    select cte1.dt,
	   cte1.val,
	   cte1.rn,
	   cte2.total + cte1.val
    from cte2
    inner join cte1
    	on cte1.rn = cte2.rn + 1
    )
select dt,
       val, 
       total 
from cte2
order by dt;


-- cross apply (SQL Server, SQL Server) or lateral (MySQL, PostgreSQL)
select s.*,
       t2.total
from test_simple s
cross apply (select coalesce(sum(t2.val), 0) as total
             from test_simple t2
	     where t2.dt <= s.dt
) t2
order by s.dt;



--- Division 2: Solutions for grouped data ---

-- Window functions
select g.*,
       coalesce(sum(g.val) over (partition by g.grp order by g.dt
                rows between unbounded preceding and current row), 
                0) as total
from test_groups g
order by g.grp, g.dt;


-- Correlated subquery
select g.*,
	(select coalesce(sum(t2.val), 0) as total
	from test_groups t2
	where g.grp = t2.grp
				and t2.dt <= g.dt) as total
from test_groups g
order by g.grp, g.dt;


-- Inner join
select g.*, 
       coalesce(sum(t2.val), 0) as total
from test_groups g
inner join test_groups t2
	on g.grp = t2.grp
		and t2.dt <= g.dt
group by g.grp, 
	 g.dt, 
	 g.val
order by g.grp, 
	 g.dt;


-- Cartesian join
select g.*, 
       coalesce(sum(t2.val), 0) as total
from test_groups g,
     test_groups t2
where g.grp = t2.grp
		and t2.dt <= g.dt
group by g.grp, 
	 g.dt, 
	 g.val
order by g.grp, 
	 g.dt;


-- Recursive query
-- required condition: no gaps in dt field
with cte (dt,
          grp,
          val,
          total)
as
   (select g.dt,
           g.grp,
	   g.val, 
	   g.val as total
    from test_groups g
    where g.dt = (select min(dt) from test_groups where grp = g.grp)
			
    union all
			
    select r.dt,
		r.grp,
	   r.val,
	   cte.total + r.val 
    from cte
    inner join test_groups r
    	-- r.dt = dateadd(day, 1, cte.dt) in SQL Server, r.dt = cte.dt + 1 in Oracle, etc
      on r.dt = dateadd(day, 1, cte.dt)
		and cte.grp = r.grp
    )
select dt,
       grp,
       val, 
       total 
from cte
order by grp, 
	 dt;


-- Recursive query & window function row_number
-- here gaps in dt field became acceptable
with cte1 (dt,
           grp,
           val,
	   rn)
as (select dt,
           grp,
           val,
	   row_number() over (partition by grp order by dt) as rn
   from test_groups),
cte2 (dt,
      grp,
      val,
      rn,
      total)
as
   (select dt,
           grp,
	   val,
	   rn,
	   val as total
    from cte1
    where rn = 1
			
    union all
			
    select cte1.dt,
	   cte1.grp,
	   cte1.val,
	   cte1.rn,
	   cte2.total + cte1.val
    from cte2
    inner join cte1
    	on cte1.grp = cte2.grp
		    and cte1.rn = cte2.rn + 1
    )
select dt,
       grp,
       val, 
       total 
from cte2
order by grp,
         dt;


-- CROSS APPLY (SQL Server, SQL Server) /  LATERAL JOIN (MySQL, PostgreSQL)
select g.*,
       t2.total
from test_groups g
cross apply (select coalesce(sum(t2.val), 0) as total
             from test_groups t2
	     where g.grp = t2.grp
	            and t2.dt <= g.dt
) t2
order by g.grp,
         g.dt;



--- Division 3: Vendor-specific solutions (simple case only) ---

-- Use of MODEL clause (Oracle)
select dt, val, total
from
    (select dt,
            val,
            val as total
    from test_simple) t
model
    dimension by (row_number() over (order by dt) as rn)
    measures (dt, val, total)
    rules (total[rn >= 2] = total[cv() - 1] + val[cv()])
order by dt;



-- Update to local variable (SQL Server)
declare @VarTotal int = 0;
declare @tv table
      (dt date null,
       val int null,
       total int null
       ); 

insert @tv
      (dt,
       val,
       total)
select dt,
       val,
       0 as total
from test_simple
order by dt;

update @tv
set @VarTotal = total = @VarTotal + val
from @tv;

select * from @tv order by dt;


-- Cursor (SQL Server)
create table #temp
      (dt date primary key,
       val int null,
       total int null
       ); 


insert #temp
	  (dt,
	   val)
select dt,
	   val	   
from test_simple
order by dt;

declare @VarTotal int,
        @VarDT date,
        @VarVal int;

set @VarTotal = 0;

declare cur cursor local static read_only forward_only
for select dt, val from #temp order by dt;

open cur;
fetch cur into @VarDT, @VarVal;

while @@fetch_status = 0
begin
	set @VarTotal = @VarTotal + @VarVal;
	
	update #temp
	set total = @VarTotal
	where dt = @VarDT;
	
	fetch cur into  @VarDT, @VarVal;
end;

close cur;
deallocate cur;

select dt, val, total
from #temp
order by dt;

drop table #temp;
