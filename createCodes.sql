show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'build lookups start time: ' || systimestamp from dual;

exec etl_helper.create_code_tables('storet');

select 'build lookups end time: ' || systimestamp from dual;
