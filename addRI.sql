show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'add RI start time: ' || systimestamp from dual;

exec etl_helper.add_ri('storet');

select 'add RI end time: ' || systimestamp from dual;
