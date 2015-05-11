show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'drop RI start time: ' || systimestamp from dual;

exec etl_helper.drop_ri('storet');

select 'drop RI end time: ' || systimestamp from dual;
