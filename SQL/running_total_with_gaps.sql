/* This code is part of my tutorial on running total in SQL with gaps in data
   Blog post: habr.com/ru/post/597963/ (in Russian)

   NB: dates in ANSI format, use appropriate date format in your DMBS
*/

--- Division 0: Create testing data ---

create table product_sales (dt date null,
                            product varchar(10) null,  -- varchar2(10) in Oracle
                            sales int null
                          );
insert into product_sales (dt, product, sales) values ('2021-12-01', 'A', 10);
insert into product_sales (dt, product, sales) values ('2021-12-01', 'B', 20);
insert into product_sales (dt, product, sales) values ('2021-12-05', 'C', 50);
insert into product_sales (dt, product, sales) values ('2021-12-10', 'A', 30);
insert into product_sales (dt, product, sales) values ('2021-12-12', 'B', 40);
insert into product_sales (dt, product, sales) values ('2021-12-15', 'C', 10);
insert into product_sales (dt, product, sales) values ('2021-12-20', 'A', 20);
insert into product_sales (dt, product, sales) values ('2021-12-25', 'B', 50);
insert into product_sales (dt, product, sales) values ('2021-12-31', 'B', 30);

create table dim_dt (dt date not null);
insert into dim_dt (dt) values ('2021-12-01');
insert into dim_dt (dt) values ('2021-12-02');
insert into dim_dt (dt) values ('2021-12-03');
insert into dim_dt (dt) values ('2021-12-04');
insert into dim_dt (dt) values ('2021-12-05');
insert into dim_dt (dt) values ('2021-12-06');
insert into dim_dt (dt) values ('2021-12-07');
insert into dim_dt (dt) values ('2021-12-08');
insert into dim_dt (dt) values ('2021-12-09');
insert into dim_dt (dt) values ('2021-12-10');
insert into dim_dt (dt) values ('2021-12-11');
insert into dim_dt (dt) values ('2021-12-12');
insert into dim_dt (dt) values ('2021-12-13');
insert into dim_dt (dt) values ('2021-12-14');
insert into dim_dt (dt) values ('2021-12-15');
insert into dim_dt (dt) values ('2021-12-16');
insert into dim_dt (dt) values ('2021-12-17');
insert into dim_dt (dt) values ('2021-12-18');
insert into dim_dt (dt) values ('2021-12-19');
insert into dim_dt (dt) values ('2021-12-20');
insert into dim_dt (dt) values ('2021-12-21');
insert into dim_dt (dt) values ('2021-12-22');
insert into dim_dt (dt) values ('2021-12-23');
insert into dim_dt (dt) values ('2021-12-24');
insert into dim_dt (dt) values ('2021-12-25');
insert into dim_dt (dt) values ('2021-12-26');
insert into dim_dt (dt) values ('2021-12-27');
insert into dim_dt (dt) values ('2021-12-28');
insert into dim_dt (dt) values ('2021-12-29');
insert into dim_dt (dt) values ('2021-12-30');
insert into dim_dt (dt) values ('2021-12-31');

create table dim_product (product varchar(10) not null);
insert into dim_product (product) values ('A');
insert into dim_product (product) values ('B');
insert into dim_product (product) values ('C');

--- Division 1: Solution using pre-defined dimension tables ---

-- using CTE
with ideal_combination as
    (select 
             d.dt
           , p.product
    from dim_dt d
    cross join dim_product p
    where d.dt between '2021-12-01' and '2021-12-31')
select
        i.dt
      , i.product
      , coalesce(ps.sales, 0) as sales
      , coalesce(sum(ps.sales) over (partition by i.product order by i.dt), 0) as sales_total
from ideal_combination i
left join product_sales ps
   on i.dt = ps.dt
      and i.product = ps.product
;

-- without CTE
select
        d.dt
      , p.product
      , coalesce(ps.sales, 0) as sales
      , coalesce(sum(ps.sales) over (partition by p.product order by d.dt), 0) as sales_total
from dim_dt d
cross join dim_product p
left join product_sales ps
   on d.dt = ps.dt
      and p.product = ps.product
where d.dt between '2021-12-01' and '2021-12-31';

--- Division 1: Solution using set generation in query ---

-- PostgreSQL, generate_series function
select
        d.dt
      , p.product
      , coalesce(ps.sales, 0) as sales
      , coalesce(sum(ps.sales) over (partition by p.product order by d.dt), 0) as sales_total
from (select generate_series('2021-12-01', '2021-12-31', interval '1 day')::date as dt) d
cross join (select distinct product from product_sales) p
left join product_sales ps
   on d.dt = ps.dt
      and p.product = ps.product
;

-- PostgreSQL, recursive CTE
with recursive dates_range (dt) as
    (
    select '2021-12-01'::date as dt
    union all
    select (dt + interval '1 day')::date
    from dates_range
    where dt <= '2021-12-31'::date
    )
select
        d.dt
      , p.product
      , coalesce(ps.sales, 0) as sales
      , coalesce(sum(ps.sales) over (partition by p.product order by d.dt), 0) as sales_total
from dates_range d
cross join (select distinct product from product_sales) p
left join product_sales ps
   on d.dt = ps.dt
      and p.product = ps.product
;

-- SQL Server, recursive CTE
with dates_range (dt) as
    (
    select convert(date, '2021-12-01', 102) as dt
    union all
    select dateadd(day, 1, dt) 
    from dates_range
    where dt <=  convert(date, '2021-12-31', 102)
    )
select
        d.dt
      , p.product
      , coalesce(ps.sales, 0) as sales
      , coalesce(sum(ps.sales) over (partition by p.product order by d.dt), 0) as sales_total
from dates_range d
cross join (select distinct product from product_sales) p
left join product_sales ps
   on d.dt = ps.dt
      and p.product = ps.product
;

-- Oracle DB, hierarchical query
select
        d.dt
      , p.product
      , coalesce(ps.sales, 0) as sales
      , coalesce(sum(ps.sales) over (partition by p.product order by d.dt), 0) as sales_total
from 
    (
    select (to_date('2021-12-31', 'YYYY-MM-DD') - level + 1) as dt
    from dual
    connect by level <= (to_date('2021-12-31', 'YYYY-MM-DD') - to_date('2021-12-01', 'YYYY-MM-DD') + 1)
    ) d
cross join (select distinct product from product_sales) p
left join product_sales ps
   on d.dt = ps.dt
      and p.product = ps.product
;
