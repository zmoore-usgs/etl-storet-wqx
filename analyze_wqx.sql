show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'analyze wqx tables start time: ' || systimestamp from dual;

begin
end;
/

select 'analyze wqx tables end time: ' || systimestamp from dual;
