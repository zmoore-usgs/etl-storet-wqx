show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'validate dw start time: ' || systimestamp from dual;

exec etl_helper.validate(3);

select 'validate dw tables end time: ' || systimestamp from dual;
