/* create a temporary table: this will be removed at the end of the session */

create temp table parts (
  id serial primary key,
  price integer
);

create temp table Employee(
  emp_id serial primary key,
  dep_id integer,
  manager_id integer,
  name varchar(50),
  salary integer
);

create temp table Bonus(
  bonus_id serial primary key,
  emp_id integer,
  ga_id integer,
  amount integer);

create index bonus_emp_id on Bonus (emp_id);

insert into Bonus( emp_id ) 
(
  select (floor(10 + 90*random()))
  from generate_series(1, 1000)
);

insert into Bonus ( ga_id ) 
(
  select (floor(10 + 90*random()))
  from generate_series(1, 1000)
);
insert into Bonus ( amount ) 
(
  select (floor(10 + 90*random()))
  from generate_series(1, 1000)
);

create temp table GrantingAuthority(
  ga_id serial primary key,
  emp_id integer,
  dept_id integer);

insert into GrantingAuthority ( emp_id ) 
(
  select (floor(10 + 90*random()))
  from generate_series(1, 1000)
);

insert into GrantingAuthority ( dept_id ) 
(
  select (floor(10 + 90*random()))
  from generate_series(1, 1000)
);

/* create 1000 parts with random prices between 10 and 100 */
create temp table Department (
  dept_id serial primary key,
  thing integer
);

insert into Department (thing) (
  select (floor(10 + 90*random()))
  from generate_series(1, 1000)
);


-- insert into Department (
  -- name
-- )  values (
  -- 'foo'
-- );

/* ensure that the catalog is up to date */
-- vacuum full;

-- create temp table Department (
  -- dep_idt serial primary key,
  -- thing integer
-- );

-- create temp table GrantingAuthority(
  -- ga_id serial primary key,
  -- emp_id integer,
  -- dept_id integer);

-- create temp table Bonus(
  -- bonus_id serial primary key,
  -- emp_id integer,
  -- ga_id integer,
  -- amount integer);


/* run some kind of query over it */
-- explain analyze

explain
select B.amount, D.dept_id, GA.emp_id
from Department D
join GrantingAuthority GA
on D.dept_id = GA.dept_id
join Bonus B
on B.emp_id = GA.emp_id;

-- ) as first
-- join (
  -- select * from Bonus B
-- ) as second
-- on B.ga_id = C.ga_id
-- group by C.dept_id;

-- select B.amount, GA.ga_id
-- from Bonus B, GrantingAuthority GA
-- where B.ga_id = GA.ga_id;
-- and B.amount = 500;

/* see some stats */
-- select avg_width, most_common_vals from pg_stats where tablename='parts' and attname='price'; 
